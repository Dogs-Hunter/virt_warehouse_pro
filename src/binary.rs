use anyhow::{Context, Result};
use crate::model::Operation;

const OP_MAGIC: &[u8; 4] = b"WOP1";
const BATCH_MAGIC: &[u8; 4] = b"WBT1";
const LOG_MAGIC: &[u8; 4] = b"WLG1";

pub enum BinaryLogRecord { Fence { epoch: u64, owner: String }, Operations { epoch: u64, operations: Vec<Operation> } }

pub fn encode_operation(operation: &Operation) -> Result<Vec<u8>> {
    let mut out=Vec::with_capacity(48+operation.operation_id.len()+operation.owner_id.len()+operation.sku.len());out.extend_from_slice(OP_MAGIC);put_operation(&mut out,operation)?;Ok(out)
}
pub fn decode_operation(data:&[u8])->Result<Operation>{anyhow::ensure!(data.starts_with(OP_MAGIC),"not binary operation");let mut r=Reader::new(&data[4..]);let op=r.operation()?;r.done()?;Ok(op)}

pub fn encode_batch(operations:&[Operation])->Result<Vec<u8>>{let mut out=Vec::with_capacity(16+operations.len()*64);out.extend_from_slice(BATCH_MAGIC);put_u32(&mut out,operations.len())?;for op in operations{put_operation(&mut out,op)?;}Ok(out)}
pub fn decode_batch(data:&[u8])->Result<Vec<Operation>>{anyhow::ensure!(data.starts_with(BATCH_MAGIC),"not binary batch");let mut r=Reader::new(&data[4..]);let count=r.count()?;let mut result=Vec::with_capacity(count);for _ in 0..count{result.push(r.operation()?);}r.done()?;Ok(result)}

pub fn encode_log_operations(epoch:u64,operations:&[Operation])->Result<Vec<u8>>{let mut out=Vec::with_capacity(24+operations.len()*64);out.extend_from_slice(LOG_MAGIC);out.push(2);out.extend_from_slice(&epoch.to_le_bytes());put_u32(&mut out,operations.len())?;for op in operations{put_operation(&mut out,op)?;}Ok(out)}
pub fn encode_log_fence(epoch:u64,owner:&str)->Result<Vec<u8>>{let mut out=Vec::with_capacity(16+owner.len());out.extend_from_slice(LOG_MAGIC);out.push(1);out.extend_from_slice(&epoch.to_le_bytes());put_string(&mut out,owner)?;Ok(out)}
pub fn decode_log(data:&[u8])->Result<BinaryLogRecord>{anyhow::ensure!(data.starts_with(LOG_MAGIC),"not binary log record");let mut r=Reader::new(&data[4..]);let kind=r.u8()?;let epoch=r.u64()?;let record=match kind{1=>BinaryLogRecord::Fence{epoch,owner:r.string()?},2=>{let count=r.count()?;let mut operations=Vec::with_capacity(count);for _ in 0..count{operations.push(r.operation()?);}BinaryLogRecord::Operations{epoch,operations}},_=>anyhow::bail!("unknown binary log record kind")};r.done()?;Ok(record)}

fn put_u32(out:&mut Vec<u8>,value:usize)->Result<()>{let value=u32::try_from(value).context("count exceeds u32")?;out.extend_from_slice(&value.to_le_bytes());Ok(())}
fn put_string(out:&mut Vec<u8>,value:&str)->Result<()>{let length=u16::try_from(value.len()).context("string exceeds u16")?;out.extend_from_slice(&length.to_le_bytes());out.extend_from_slice(value.as_bytes());Ok(())}
fn put_operation(out:&mut Vec<u8>,op:&Operation)->Result<()>{put_string(out,&op.operation_id)?;put_string(out,&op.owner_id)?;put_string(out,&op.sku)?;out.extend_from_slice(&op.delta.to_le_bytes());out.extend_from_slice(&op.event_version.to_le_bytes());Ok(())}

struct Reader<'a>{data:&'a[u8],cursor:usize}
impl<'a> Reader<'a>{
 fn new(data:&'a[u8])->Self{Self{data,cursor:0}}
 fn take(&mut self,n:usize)->Result<&'a[u8]>{let end=self.cursor.checked_add(n).context("binary offset overflow")?;anyhow::ensure!(end<=self.data.len(),"binary payload truncated");let result=&self.data[self.cursor..end];self.cursor=end;Ok(result)}
 fn u8(&mut self)->Result<u8>{Ok(self.take(1)?[0])}
 fn u16(&mut self)->Result<u16>{Ok(u16::from_le_bytes(self.take(2)?.try_into()?))}
 fn u32(&mut self)->Result<u32>{Ok(u32::from_le_bytes(self.take(4)?.try_into()?))}
 fn u64(&mut self)->Result<u64>{Ok(u64::from_le_bytes(self.take(8)?.try_into()?))}
 fn i64(&mut self)->Result<i64>{Ok(i64::from_le_bytes(self.take(8)?.try_into()?))}
 fn count(&mut self)->Result<usize>{let n=self.u32()? as usize;anyhow::ensure!(n<=100_000,"binary batch count is unreasonable");Ok(n)}
 fn string(&mut self)->Result<String>{let n=self.u16()? as usize;Ok(std::str::from_utf8(self.take(n)?)?.to_owned())}
 fn operation(&mut self)->Result<Operation>{Ok(Operation{operation_id:self.string()?,owner_id:self.string()?,sku:self.string()?,delta:self.i64()?,event_version:self.u64()?})}
 fn done(&self)->Result<()>{anyhow::ensure!(self.cursor==self.data.len(),"binary payload has trailing bytes");Ok(())}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operation(id: &str) -> Operation {
        Operation { operation_id: id.into(), owner_id: "owner".into(), sku: "sku".into(), delta: 17, event_version: 1 }
    }

    #[test]
    fn operation_and_batch_round_trip() {
        let first = operation("op-1");
        assert_eq!(decode_operation(&encode_operation(&first).unwrap()).unwrap(), first);
        let values = vec![operation("op-1"), operation("op-2")];
        assert_eq!(decode_batch(&encode_batch(&values).unwrap()).unwrap(), values);
    }

    #[test]
    fn rejects_truncation_and_trailing_bytes() {
        let mut encoded = encode_operation(&operation("op-1")).unwrap();
        assert!(decode_operation(&encoded[..encoded.len() - 1]).is_err());
        encoded.push(0);
        assert!(decode_operation(&encoded).is_err());
    }

    #[test]
    fn replicated_records_round_trip() {
        match decode_log(&encode_log_operations(7, &[operation("op-1")]).unwrap()).unwrap() {
            BinaryLogRecord::Operations { epoch, operations } => {
                assert_eq!(epoch, 7);
                assert_eq!(operations, vec![operation("op-1")]);
            }
            _ => panic!("unexpected log record"),
        }
        match decode_log(&encode_log_fence(8, "writer-1").unwrap()).unwrap() {
            BinaryLogRecord::Fence { epoch, owner } => {
                assert_eq!(epoch, 8);
                assert_eq!(owner, "writer-1");
            }
            _ => panic!("unexpected log record"),
        }
    }
}
