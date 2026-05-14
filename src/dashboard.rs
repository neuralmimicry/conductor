use axum::response::Html;

pub fn render_dashboard(title: &str, version: &str) -> Html<String> {
    Html(
        include_str!("../assets/dashboard.html")
            .replace("__TITLE__", title)
            .replace("__VERSION__", version),
    )
}
