use fly_into_the_maelstrom::*;
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct EchoBody {
    msg_id: MessageId,
    echo: Box<str>,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum MessageBody {
    Echo(EchoBody),
}

#[test]
fn deserialize_custom_body() {
    let json_string = r#"{
            "src": "c1",
            "dest": "n3",
            "body": {
                "type": "echo",
                "msg_id": 1,
                "echo": "Please echo 35"
            }
        }"#;
    let message: Message<MessageBody> = serde_json::from_str(json_string).unwrap();
    let MessageBody::Echo(echo) = message.body;
    assert_eq!(echo.echo.as_ref(), "Please echo 35");
}
