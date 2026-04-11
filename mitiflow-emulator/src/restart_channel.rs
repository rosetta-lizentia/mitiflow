//! Restart channel for scheduling component restart requests.

use std::time::Duration;

use tokio::sync::mpsc;

/// Request to restart a component instance after a delay.
#[derive(Debug, Clone)]
pub struct RestartRequest {
    pub component: String,
    pub instance: Option<usize>,
    pub delay: Duration,
}

pub type RestartSender = mpsc::Sender<RestartRequest>;
pub type RestartReceiver = mpsc::Receiver<RestartRequest>;

/// Create bounded restart request channel.
pub fn create_restart_channel() -> (RestartSender, RestartReceiver) {
    mpsc::channel(32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn creates_restart_channel() {
        let (_tx, _rx) = create_restart_channel();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sends_and_receives_restart_request() {
        let (tx, mut rx) = create_restart_channel();

        let request = RestartRequest {
            component: "producer-a".to_string(),
            instance: Some(2),
            delay: Duration::from_millis(250),
        };

        tx.send(request.clone()).await.expect("send should succeed");

        let received = rx.recv().await.expect("request should be received");
        assert_eq!(received.component, request.component);
        assert_eq!(received.instance, request.instance);
        assert_eq!(received.delay, request.delay);
    }
}
