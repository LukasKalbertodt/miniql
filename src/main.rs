use futures::future;
use hyper::{
    rt::{self, Future},
    service::service_fn,
    Body, Method, Response, Server, StatusCode,
};
use juniper::{
    EmptyMutation, LookAheadMethods, RootNode, FieldResult
};
use std::sync::{Mutex, Arc};
use postgres::{Client, NoTls, Row};
use fallible_iterator::FallibleIterator;


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
    db: Mutex<Client>,
}

impl juniper::Context for Context {}

struct Query;


#[juniper::object(Context = Context)]
impl Query {

    fn apiVersion() -> &str {
        "1.0"
    }

    fn series(context: &Context) -> FieldResult<Vec<Series>> {
        let result = context.db.lock()?
            .query_raw("select * from series", std::iter::empty())?
            .map(|row| Ok(Series::from_row(row)))
            .collect()?;

        Ok(result)
    }

    fn event(context: &Context, executor: &Executor) -> FieldResult<Vec<Event>> {
        let result = if executor.look_ahead().child_names().contains(&"partOf") {
            context.db.lock()?
                .query_raw(
                    "select events.id, events.title, series.id, series.name, series.description \
                     from events left join series on events.part_of = series.id",
                    std::iter::empty(),
                )?
                .map(|row| Ok(Event::from_row_with_series(row)))
                .collect()?
        } else {
            context.db.lock()?
                .query_raw("select * from events", std::iter::empty())?
                .map(|row| Ok(Event::from_row(row)))
                .collect()?
        };
        Ok(result)
    }
}


// ===== HTTP Server and init stuf ============================================


fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();

    let connection_params = "host=localhost dbname=minitest port=5555 user=postgres password=test";
    let db = Client::connect(connection_params, NoTls)?;


    let addr = ([127, 0, 0, 1], 3000).into();

    // TODO: this is terrible. We should use a proper connection pool.
    let db = Mutex::new(db);
    let ctx = Arc::new(Context { db });
    let root_node = Arc::new(RootNode::new(Query, EmptyMutation::<Context>::new()));

    let new_service = move || {
        let root_node = root_node.clone();
        let ctx = ctx.clone();
        service_fn(move |req| -> Box<dyn Future<Item = _, Error = _> + Send> {
            let root_node = root_node.clone();
            let ctx = ctx.clone();
            match (req.method(), req.uri().path()) {
                (&Method::GET, "/") => Box::new(juniper_hyper::graphiql("/graphql")),
                (&Method::GET, "/graphql") => Box::new(juniper_hyper::graphql(root_node, ctx, req)),
                (&Method::POST, "/graphql") => {
                    Box::new(juniper_hyper::graphql(root_node, ctx, req))
                }
                _ => {
                    let mut response = Response::new(Body::empty());
                    *response.status_mut() = StatusCode::NOT_FOUND;
                    Box::new(future::ok(response))
                }
            }
        })
    };
    let server = Server::bind(&addr)
        .serve(new_service)
        .map_err(|e| eprintln!("server error: {}", e));
    println!("Listening on http://{}", addr);

    rt::run(server);

    Ok(())
}
