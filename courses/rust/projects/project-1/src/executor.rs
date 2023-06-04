use std::future::Future;

#[derive(Copy, Clone)]
pub struct GlommioExec;

impl<Fut> hyper::rt::Executor<Fut> for GlommioExec
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        monoio::spawn(fut);
    }
}
