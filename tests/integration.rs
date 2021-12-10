mod utils;

use anyhow::Context;
use anyhow::Result;
use redis::RedisError;
use utils::{get_redis_connection, start_redis_server};

#[test]
fn test_hello() -> Result<()> {
    let _guards = vec![start_redis_server(6279)
        .with_context(|| "failed to start redis server")?];
    let mut con =
        get_redis_connection(6279).with_context(|| "failed to connect to redis server")?;

    let res: Vec<i32> = redis::cmd("hello.mul")
        .arg(&[3, 4])
        .query(&mut con)
        .with_context(|| "failed to run hello.mul")?;
    assert_eq!(res, vec![3, 4, 12]);

    println!("{:?}", res);

    let res: Result<Vec<i32>, RedisError> =
        redis::cmd("hello.mul").arg(&["3", "xx"]).query(&mut con);
    if let Ok(_) = res {
        return Err(anyhow::Error::msg("Should return an error"));
    }

    Ok(())
}