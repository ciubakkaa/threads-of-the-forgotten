#[derive(Debug)]
pub enum ServerError {
    Io(std::io::Error),
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "server io error: {err}"),
        }
    }
}

impl std::error::Error for ServerError {}

impl From<std::io::Error> for ServerError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug)]
struct HttpApiError {
    status: StatusCode,
    error: ApiError,
}

impl HttpApiError {
    fn run_not_found(requested_run_id: &str, active_run_id: Option<&str>) -> Self {
        let details = active_run_id
            .map(|active| format!("requested_run_id={requested_run_id} active_run_id={active}"));
        Self {
            status: StatusCode::NOT_FOUND,
            error: ApiError::new(
                ErrorCode::RunNotFound,
                "run_id does not match an active run",
                details,
            ),
        }
    }

    fn invalid_query(message: impl Into<String>, details: Option<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            error: ApiError::new(ErrorCode::InvalidQuery, message, details),
        }
    }

    fn invalid_command(message: impl Into<String>, details: Option<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            error: ApiError::new(ErrorCode::InvalidCommand, message, details),
        }
    }

    fn internal(message: impl Into<String>, details: Option<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error: ApiError::new(ErrorCode::InternalError, message, details),
        }
    }

    fn from_persistence(err: PersistenceError) -> Self {
        match err {
            PersistenceError::NotAttached => {
                Self::invalid_query("persistence store is not attached", None)
            }
            PersistenceError::RunAlreadyExists(run_id) => Self {
                status: StatusCode::CONFLICT,
                error: ApiError::new(
                    ErrorCode::RunStateConflict,
                    "run_id already exists; pass replace_existing=true to replace",
                    Some(format!("run_id={run_id}")),
                ),
            },
            other => Self::internal("persistence operation failed", Some(other.to_string())),
        }
    }
}

impl IntoResponse for HttpApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.error)).into_response()
    }
}

