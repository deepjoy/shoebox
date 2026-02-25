use axum::{
    middleware,
    routing::{delete, get, head, post, put},
    Router,
};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::request_id::{MakeRequestUuid, SetRequestIdLayer};
use tower_http::trace::TraceLayer;

use crate::auth;
use crate::handlers;
use crate::services::AppState;

pub fn create_router(state: AppState) -> Router {
    let credential_provider = state.credential_provider.clone();
    let bucket_names = state.bucket_names.clone();

    Router::new()
        // Service-level: GET / → ListBuckets
        .route("/", get(handlers::bucket::list_buckets))
        // Admin endpoints
        .route(
            "/_shoebox/credentials",
            post(crate::api::credentials::create_credential)
                .get(crate::api::credentials::list_credentials),
        )
        .route(
            "/_shoebox/credentials/{access_key_id}",
            delete(crate::api::credentials::delete_credential),
        )
        .route(
            "/_shoebox/reload",
            post(crate::api::credentials::reload_config),
        )
        .route("/_shoebox/scan/status", get(crate::api::scan::scan_status))
        // Bucket-level
        .route("/{bucket}", head(handlers::bucket::head_bucket))
        .route("/{bucket}", get(handlers::bucket::bucket_or_list))
        .route("/{bucket}", post(handlers::bucket::post_bucket_dispatcher))
        // Object-level
        .route("/{bucket}/{*key}", get(handlers::object::get_object))
        .route("/{bucket}/{*key}", put(handlers::object::put_object))
        .route("/{bucket}/{*key}", post(handlers::object::post_object))
        .route("/{bucket}/{*key}", delete(handlers::object::delete_object))
        .route("/{bucket}/{*key}", head(handlers::object::head_object))
        .with_state(state)
        // Innermost → outermost:
        .layer(TraceLayer::new_for_http())
        .layer(RequestBodyLimitLayer::new(5 * 1024 * 1024 * 1024)) // 5GB S3 PUT limit
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
        .layer(middleware::from_fn_with_state(
            credential_provider,
            auth::auth_middleware,
        ))
        .layer(middleware::from_fn_with_state(
            bucket_names,
            auth::virtual_host_middleware,
        ))
}
