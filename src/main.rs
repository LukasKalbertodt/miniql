use futures::future;
use hyper::{
    rt::{self, Future},
    service::service_fn,
    Body, Method, Response, Server, StatusCode,
};
use juniper::{
    EmptyMutation, RootNode, FieldResult
};
use std::sync::{Mutex, Arc};
use postgres::{Client, NoTls, Row};
use fallible_iterator::FallibleIterator;



#[derive(juniper::GraphQLObject)]
struct Series {
    id: i32,
    name: String,
    description: Option<String>,
}

impl Series {
    fn from_row(row: Row) -> Self {
        Self {
            id: row.get(0),
            name: row.get(1),
            description: row.get(2),
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
            .query_raw("SELECT * FROM series", std::iter::empty())?
            .map(|row| Ok(Series::from_row(row)))
            .collect()?;

        Ok(result)
    }
}




fn main() {
    pretty_env_logger::init();

    let db = Client::connect("host=localhost dbname=minitest port=5555 user=postgres password=test", NoTls).unwrap();


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
}
