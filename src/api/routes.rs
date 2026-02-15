use axum::{
    routing::{delete, get, head, post, put},
    Router,
};

use crate::handlers;
use crate::services::AppState;

pub fn create_router(state: AppState) -> Router {
    Router::new()
        // Service-level: GET / → ListBuckets
        .route("/", get(handlers::bucket::list_buckets))
        // Bucket-level
        .route("/{bucket}", head(handlers::bucket::head_bucket))
        .route("/{bucket}", get(handlers::bucket::bucket_or_list))
        .route("/{bucket}", post(handlers::bucket::post_bucket_dispatcher))
        // Object-level
        .route("/{bucket}/{*key}", get(handlers::object::get_object))
        .route("/{bucket}/{*key}", put(handlers::object::put_object))
        .route("/{bucket}/{*key}", delete(handlers::object::delete_object))
        .route("/{bucket}/{*key}", head(handlers::object::head_object))
        .with_state(state)
}
