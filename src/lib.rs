#[macro_use]
extern crate redis_module;

use chrono::Utc;
use redis_module::raw::KeyType;
use redis_module::{
    Context, NextArg, RedisError, RedisResult, RedisString, RedisValue, ThreadSafeContext,
};
use std::time::Duration;
use std::{result, thread};

fn hello_mul(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let blocked_client = ctx.block_client();

    let mut args = args.into_iter().skip(1);
    let key_prefix = args.next_arg()?.to_string();
    let concurrents = args.next_arg()?.to_string();
    let task_timeout = args.next_arg()?.parse_integer()?;

    thread::spawn(move || {
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);

        /*
        // get free and work key
        let ctx = thread_ctx.lock();
        let free = RedisString::create(ctx.ctx, &format!("{}_free", prefix));
        let work = RedisString::create(ctx.ctx, &format!("{}_work", prefix));
        let free_list = ctx.open_key_writable(&free);
        let work_list = ctx.open_key_writable(&work);
        drop(ctx);

        // check key type
        let free_list_type = free_list.key_type();
        let work_list_type = work_list.key_type();
        if (free_list_type != KeyType::Empty && free_list_type != KeyType::List)
        || (work_list_type != KeyType::Empty && work_list_type != KeyType::List)
        {
            thread_ctx.reply(Err(RedisError::WrongType));
        }
        */

        let ctx = thread_ctx.lock();
        let free_key = &format!("{}_free", key_prefix);

        let work_key = &format!("{}_work", key_prefix);

        let mut flag = "free_and_work";
        let mut timeout = 1;
        loop {
            let now_ts = Utc::now().timestamp();

            let ctx = thread_ctx.lock();

            if flag == "free_and_work" {
                let result = ctx.call("BLPOP", &[free_key, work_key, "0"]);

                match result {
                    Ok(RedisValue::Array(result)) => match &result[..] {
                        [RedisValue::BulkRedisString(r_key), RedisValue::BulkRedisString(r_element)] =>
                        {
                            let key = r_key.to_string();
                            let element = r_element.to_string();
                            let ts = r_element.parse_integer().unwrap();

                            if key.ends_with("_work") {
                                let retry_after = task_timeout - (now_ts - ts);

                                if retry_after > 0 {
                                    ctx.call("RPUSH", &[&key, &element]);

                                    flag = "free";
                                    timeout = retry_after;
                                }
                            }
                        }
                        _ => unreachable!(),
                    },
                    _ => unreachable!(),
                }
            } else {
                let result = ctx.call("BLPOP", &[free_key, timeout]);
            }

            /*
            match free_list.list_pop_head() {
                None => {
                    thread_ctx.reply(Ok(RedisValue::Null))
                }
                Some(value) => {
                    let ret_cpy = value.clone();
                    thread_ctx.reply(Ok(RedisValue::BulkString(ret_cpy.into())))
                }
            };
            */
        }
    });

    // We will reply later, from the thread
    Ok(RedisValue::NoReply)
}

//////////////////////////////////////////////////////

redis_module! {
    name: "hello",
    version: 1,
    data_types: [],
    commands: [
        ["hello.mul", hello_mul, "", 0, 0, 0],
    ],
}
