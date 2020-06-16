use futures::TryStreamExt;
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Method, Response, Server, StatusCode,
};
use juniper::{
    EmptyMutation, LookAheadMethods, RootNode, FieldResult, EmptySubscription
};
use std::{
    sync::Arc,
    str::FromStr,
};
use tokio_postgres::{Config, NoTls, Row, Statement};
use mobc::Pool;
use mobc_postgres::PgConnectionManager;


// ===== API definition ======================================================

#[derive(juniper::GraphQLObject)]
struct Series {
    id: i32,
    name: String,
    description: Option<String>,
}

impl Series {
    fn from_row_with_offset(row: Row, offset: usize) -> Self {
        Self {
            id: row.get(offset + 0),
            name: row.get(offset + 1),
            description: row.get(offset + 2),
        }
    }

    fn from_row(row: Row) -> Self {
        Self::from_row_with_offset(row, 0)
    }
}

#[derive(juniper::GraphQLObject)]
struct Event {
    id: i32,
    title: String,
    part_of: Option<Series>,
}

impl Event {
    fn from_row(row: Row) -> Self {
        Self {
            id: row.get(0),
            title: row.get(1),
            part_of: None,
        }
    }

    fn from_row_with_series(row: Row) -> Self {
        Self {
            id: row.get(0),
            title: row.get(1),
            part_of: row.get::<_, Option<i32>>(2).map(
                |_| Series::from_row_with_offset(row, 2)
            )
        }
    }
}

struct Context {
    db: Pool<PgConnectionManager<NoTls>>,
    series_query: Statement,
}

impl juniper::Context for Context {}

struct Query;


#[juniper::graphql_object(Context = Context)]
impl Query {

    fn apiVersion() -> &str {
        "1.0"
    }

    async fn series(context: &Context) -> FieldResult<Vec<Series>> {
        let before = std::time::Instant::now();
        let out = context.db.get().await?
            .query_raw(&context.series_query, std::iter::empty()).await?
            .map_ok(Series::from_row)
            .try_collect().await?;

        // pin_mut!(rows);
        // let out = rows.map_ok(Series::from_row).try_collect().await?;
        println!("{:?}", before.elapsed());

        Ok(out)
    }

    async fn event(context: &Context, executor: &Executor) -> FieldResult<Vec<Event>> {
        let result = if executor.look_ahead().children().iter().any(|c| c.field_name() == "partOf") {
            context.db.get().await?
                .query_raw(
                    "select events.id, events.title, series.id, series.name, series.description \
                     from events left join series on events.part_of = series.id",
                    std::iter::empty(),
                ).await?
                .map_ok(Event::from_row_with_series)
                .try_collect().await?
        } else {
            context.db.get().await?
                .query_raw("select * from events", std::iter::empty()).await?
                .map_ok(Event::from_row)
                .try_collect().await?
        };
        Ok(result)
    }
}


// ===== HTTP Server and init stuf ============================================


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();

    // let connection_params = "host=localhost dbname=minitest port=5555 user=postgres password=test";
    // let mut db = Client::connect(connection_params, NoTls)?;

    let config = Config::from_str("postgres://postgres:test@localhost:5555/minitest")?;
    let manager = PgConnectionManager::new(config, NoTls);
    let pool = Pool::builder().max_open(20).build(manager);

    let series_query = pool.get().await?.prepare("select * from series").await?;

    let addr = ([127, 0, 0, 1], 3000).into();

    // TODO: this is terrible. We should use a proper connection pool.
    let ctx = Arc::new(Context { db: pool, series_query });
    let root_node = Arc::new(RootNode::new(
        Query,
        EmptyMutation::<Context>::new(),
        EmptySubscription::<Context>::new(),
    ));

    let new_service = make_service_fn(move |_| {
        let root_node = root_node.clone();
        let ctx = ctx.clone();

        async move {
            Ok::<_, hyper::Error>(service_fn(move |req| {
                let root_node = root_node.clone();
                let ctx = ctx.clone();
                async move {
                    match (req.method(), req.uri().path()) {
                        (&Method::GET, "/") => juniper_hyper::graphiql("/graphql", None).await,
                        (&Method::GET, "/graphql") | (&Method::POST, "/graphql") => {
                            juniper_hyper::graphql(root_node, ctx, req).await
                        }
                        _ => {
                            let mut response = Response::new(Body::empty());
                            *response.status_mut() = StatusCode::NOT_FOUND;
                            Ok(response)
                        }
                    }
                }
            }))
        }
    });

    let server = Server::bind(&addr).serve(new_service);
    println!("Listening on http://{}", addr);

    if let Err(e) = server.await {
        eprintln!("server error: {}", e)
    }

    Ok(())
}
