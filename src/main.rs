use clap::Parser;
use csv::Writer;
use massa_api_exports::operation::OperationInput;
use massa_models::{
    address::Address,
    amount::Amount,
    config::constants,
    operation::{Operation, OperationId, OperationType},
    slot::Slot,
    timeslots::get_current_latest_block_slot,
};
use massa_sdk::{Client, ClientConfig, HttpConfig};
use massa_time::MassaTime;
use massa_wallet::Wallet;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{File, OpenOptions},
    path::PathBuf,
};
use std::{net::IpAddr, str::FromStr};

/// Send vested tokens ton an address
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// RPC IP
    #[arg(long)]
    rpc_ip: IpAddr,

    /// Wallet path
    #[arg(long)]
    wallet_path: PathBuf,

    /// Sender address
    #[arg(long)]
    sender_address: Address,

    /// Path to the CSV file containing the list of operations to send
    #[arg(long)]
    csv_path: PathBuf,

    /// Fee
    #[arg(long)]
    fee: Amount,

    /// Path to the CSV file containing the list of successfully sent operations
    #[arg(long)]
    output_path: PathBuf,
}

enum OperationStatus {
    Pending,
    Success,
    Failure,
}

/// Append a line to the output CSV file
fn append_done(
    output_writer: &mut Writer<&File>,
    op: OperationType,
    op_id: OperationId,
    result_str: String,
) {
    // CSV format: (recipient_address, amount, operation_id)
    if let OperationType::Transaction {
        recipient_address,
        amount,
    } = op
    {
        output_writer
            .write_record(&[
                recipient_address.to_string(),
                amount.to_string(),
                op_id.to_string(),
                result_str,
            ])
            .expect("could not write to output file");
    } else {
        panic!("unexpected operation type");
    }
}

/// send a batch of operations
async fn check_operations(
    client: &Client,
    ops: Vec<(OperationId, u64)>,
) -> Result<Vec<OperationStatus>, String> {
    let op_index = ops.iter().cloned().collect::<HashMap<_, _>>();

    // get current final period
    let final_period = client
        .public
        .get_status()
        .await
        .map_err(|err| format!("could not get status: {}", err))?
        .execution_stats
        .final_cursor
        .period;

    // get operation info
    let (op_ids, _): (Vec<OperationId>, Vec<u64>) = ops.into_iter().unzip();
    let ops_info = client
        .public
        .get_operations(op_ids.clone())
        .await
        .map_err(|err| format!("could not get operation status: {}", err))?;

    let mut results = HashMap::new();

    // check the status of the operations
    let mut missing_ops = op_ids.iter().cloned().collect::<HashSet<_>>();
    for op_info in ops_info {
        missing_ops.remove(&op_info.id);
        let Some(&expiry_period) = op_index.get(&op_info.id) else {
            continue;
        };

        match (op_info.op_exec_status, op_info.is_operation_final) {
            (Some(true), Some(true)) => {
                results.insert(op_info.id, OperationStatus::Success);
            }
            (Some(false), Some(true)) => {
                results.insert(op_info.id, OperationStatus::Failure);
            }
            _ => {
                if final_period >= expiry_period {
                    results.insert(op_info.id, OperationStatus::Failure);
                } else {
                    results.insert(op_info.id, OperationStatus::Pending);
                }
            }
        }
    }

    // check the status of missing ops
    for op_id in missing_ops {
        if final_period >= op_index[&op_id] {
            results.insert(op_id, OperationStatus::Failure);
        } else {
            results.insert(op_id, OperationStatus::Pending);
        }
    }

    // align results
    Ok(op_ids
        .into_iter()
        .map(|id| results.remove(&id).unwrap())
        .collect::<Vec<_>>())
}

/// send a batch of operations
async fn send_operations(
    client: &Client,
    wallet: &Wallet,
    ops: Vec<OperationType>,
    fee: Amount,
    from_addr: Address,
) -> Result<(Vec<OperationId>, u64), String> {
    // compute slot
    let slot = get_current_latest_block_slot(
        constants::THREAD_COUNT,
        constants::T0,
        *constants::GENESIS_TIMESTAMP,
    )
    .ok()
    .flatten()
    .unwrap_or_else(|| Slot::new(0, 0));
    let mut expire_period = slot.period + constants::OPERATION_VALIDITY_PERIODS;
    if slot.thread >= from_addr.get_thread(constants::THREAD_COUNT) {
        expire_period += 1;
    };

    if ops.is_empty() {
        return Ok((vec![], expire_period));
    }

    // sign ops
    let ops = ops
        .into_iter()
        .map(|op| {
            let op = wallet
                .create_operation(
                    Operation {
                        fee,
                        expire_period,
                        op,
                    },
                    from_addr,
                )
                .map_err(|err| format!("could not create operation: {}", err))?;
            Ok(OperationInput {
                creator_public_key: op.content_creator_pub_key,
                serialized_content: op.serialized_data,
                signature: op.signature,
            })
        })
        .collect::<Result<_, String>>()?;

    // send ops
    match client.public.send_operations(ops).await {
        Ok(operation_ids) => Ok((operation_ids, expire_period)),
        Err(e) => return Err(format!("could not send operation: {}", e)),
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let client_config = ClientConfig {
        max_request_body_size: 1024 * 1024 * 100,
        request_timeout: MassaTime::from_millis(10000),
        max_concurrent_requests: 16,
        certificate_store: "".into(),
        id_kind: "Number".into(),
        max_log_length: 100,
        headers: vec![],
    };

    let http_config = HttpConfig {
        client_config,
        enabled: true,
    };

    let public_port = 33035;
    let private_port = 33034;
    let grpc_port = 33037;
    let grpc_priv_port = 33038;
    let chain_id = *massa_models::config::constants::CHAINID;

    // open output file for appending
    let output_file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&args.output_path)
        .expect("could not open output file");
    let mut output_writer = Writer::from_writer(&output_file);

    // load CSV and read the rows (columns = recipient_address, amount)
    let mut send_queue: VecDeque<OperationType> = VecDeque::new();

    {
        let f = File::open(args.csv_path).expect("could not open CSV file");
        let mut rdr = csv::ReaderBuilder::new().has_headers(false).from_reader(f);
        for result in rdr.deserialize() {
            let record: (String, String) = result.expect("could not read CSV record");
            let recipient_address = Address::from_str(&record.0)
                .expect(format!("Invalid recipient address '{}'", &record.0).as_str());
            let amount = Amount::from_str(&record.1).expect(
                format!(
                    "Invalid amount '{}' for address {}",
                    &record.1, &recipient_address
                )
                .as_str(),
            );
            send_queue.push_back(OperationType::Transaction {
                recipient_address,
                amount,
            });
        }
    }
    if send_queue.is_empty() {
        println!("CSV empty: no operations to send");
        return;
    }
    println!("Loaded CSV: {} operations to send", send_queue.len());

    // connect
    let client = Client::new(
        args.rpc_ip,
        public_port,
        private_port,
        grpc_port,
        grpc_priv_port,
        chain_id,
        &http_config,
    )
    .await
    .expect("Could not initialize client");

    // ask for password
    let password = dialoguer::Password::new()
        .with_prompt("Enter wallet password")
        .interact()
        .expect("IO error: Password reading failed, walled couldn't be unlocked");

    // load wallet
    let wallet = Wallet::new(args.wallet_path.clone(), password, chain_id).expect(&format!(
        "could not load wallet {}",
        args.wallet_path.to_string_lossy().to_string()
    ));

    // check sender address
    if !wallet.keys.contains_key(&args.sender_address) {
        println!(
            "Address {} not found in the wallet {}",
            args.sender_address,
            args.wallet_path.to_string_lossy()
        );
        return;
    }

    // send chunks
    let chunk_size: usize = 50;
    let mut check_queue: Vec<(OperationId, OperationType, u64)> = Vec::new();
    let loop_interval = std::time::Duration::from_secs(16);
    let mut first_iter = true;
    let check_batch_size = 50;
    let mut n_failures = 0;

    loop {
        if send_queue.is_empty() && check_queue.is_empty() {
            break;
        }

        if !first_iter {
            tokio::time::sleep(loop_interval).await;
        }
        first_iter = false;

        // check the status of the operations that need to be checked
        let old_check_queue = std::mem::take(&mut check_queue);
        for batch in old_check_queue.chunks(check_batch_size) {
            let batch_param = batch
                .iter()
                .map(|(id, _, expiry)| (*id, *expiry))
                .collect::<Vec<_>>();
            let res = check_operations(&client, batch_param).await;
            match res {
                Ok(statuses) => {
                    for (queue_element, status) in batch.iter().cloned().zip(statuses.into_iter()) {
                        match status {
                            OperationStatus::Pending => {
                                check_queue.push(queue_element);
                            }
                            OperationStatus::Success => {
                                println!("Operation {} succeeded", queue_element.0);
                                append_done(
                                    &mut output_writer,
                                    queue_element.1,
                                    queue_element.0,
                                    "SUCCESS".to_string(),
                                );
                            }
                            OperationStatus::Failure => {
                                println!("Operation {} failed.", queue_element.0);
                                //send_queue.push_back(queue_element.1);  uncomment to retry
                                append_done(
                                    &mut output_writer,
                                    queue_element.1,
                                    queue_element.0,
                                    "FAILURE".to_string(),
                                );
                                n_failures += 1;
                            }
                        }
                    }
                }
                Err(err) => {
                    println!("Error checking operations: {}", err);
                    check_queue.extend(batch.to_vec());
                    continue;
                }
            }

            // sleep 1 second between each batch call
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }

        // send batches of operations that need to be sent
        let batch = send_queue
            .drain(..chunk_size.min(send_queue.len()))
            .collect::<Vec<_>>();
        if batch.is_empty() {
            continue;
        }
        println!("Sending batch of {} operations...", batch.len());
        let send_res = send_operations(
            &client,
            &wallet,
            batch.clone(),
            args.fee,
            args.sender_address,
        )
        .await;
        match send_res {
            Ok((op_ids, expire_period)) => {
                // the batch was sent: add it to the check queue
                println!("Sent batch of {} operations", batch.len());
                check_queue.extend(
                    op_ids
                        .into_iter()
                        .zip(batch)
                        .map(|(id, op)| (id, op, expire_period)),
                );
            }
            Err(err) => {
                // requeue the batch for sending
                println!("Error sending batch: {}", err);
                send_queue.extend(batch);
            }
        }
    }

    println!("All operations were processed.");
    if n_failures > 0 {
        println!("WARNING: {} operations failed.", n_failures);
    }
}
