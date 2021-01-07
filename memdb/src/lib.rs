use std::collections::HashMap;

#[derive(Eq, PartialEq, Hash, Clone, Debug)]
pub enum Key {
    Short(i32),
    Int(i64),
    Varchar(String)
}

pub type Value = Vec<u8>;

#[derive(Debug, PartialEq)]
pub struct Record {
    key: Key,
    value: Value
}

#[derive(Debug)]
pub enum MemDBError {
    DatabaseExists,
    DatabaseDoesNotExist,
    TransactionExists,
    TransactionDoesNotExist,
    EntryExists,
    EntryDoesNotExist,
    Failure,
}

pub type MemDBResult<T> = Result<T, MemDBError>;

pub trait Transaction {

}

pub trait Index {
    fn get(&self, key: Key, transaction: Option<&dyn Transaction>) -> &dyn Iterator<Item = Record>;
    fn get_single(&self, key: Key, transaction: Option<&dyn Transaction>) -> Record;

    fn insert(&mut self, record: Record, transaction: Option<&dyn Transaction>);

    fn remove(&mut self, record: Record, transaction: Option<&dyn Transaction>);
}

pub trait MemDB<'a> {
    fn create_index(&mut self, name: &str, key_type: Key) -> MemDBResult<()>;
    fn drop_index(&mut self, name: &str) -> MemDBResult<()>;
    fn open_index(&'a mut self, name: &str) -> MemDBResult<&'a mut dyn Index>;
    fn close_index(&mut self, index: &dyn Index) -> MemDBResult<()>;
    fn begin_transaction(&mut self, ) -> MemDBResult<&'a dyn Transaction>;
    fn abort_transaction(&mut self, transaction: &dyn Transaction) -> MemDBResult<()>;
    fn commit_transaction(&mut self, transaction: &dyn Transaction) -> MemDBResult<()>;
}

pub struct SimpleMemDB {
    indices: HashMap<String, SimpleIndex>

}

pub struct SimpleIndex {
    map: HashMap<Key, Value>
}

pub struct SimpleTransaction {

}

impl SimpleIndex {
    fn new() -> Self {
        return SimpleIndex {
            map: HashMap::new()
        }
    }
}

impl SimpleMemDB {
    fn new() -> Self {
        return SimpleMemDB {
            indices: HashMap::new()
        }
    }
}

impl<'a> MemDB<'a> for SimpleMemDB {
    fn create_index(&mut self, name: &str, key_type: Key) -> MemDBResult<()> {
        self.indices.insert(String::from(name), SimpleIndex::new());

        Ok(())
    }

    fn drop_index(&mut self, name: &str) -> MemDBResult<()> {
        unimplemented!()
    }

    fn open_index(&'a mut self, name: &str) -> MemDBResult<&'a mut dyn Index> {
        Ok(self.indices.get_mut(name).unwrap())
    }

    fn close_index(&mut self, index: &dyn Index) -> MemDBResult<()> {
        unimplemented!()
    }

    fn begin_transaction(&mut self, ) -> MemDBResult<&'a dyn Transaction> {
        unimplemented!()
    }

    fn abort_transaction(&mut self, transaction: &dyn Transaction) -> MemDBResult<()> {
        unimplemented!()
    }

    fn commit_transaction(&mut self, transaction: &dyn Transaction) -> MemDBResult<()> {
        unimplemented!()
    }
}

impl Index for SimpleIndex {
    fn get(&self, key: Key, transaction: Option<&dyn Transaction>) -> &dyn Iterator<Item=Record> {
        unimplemented!()
    }

    fn get_single(&self, key: Key, transaction: Option<&dyn Transaction>) -> Record {
        Record {
            key: key.clone(),
            value: self.map.get(&key).unwrap().to_vec()
        }
    }

    fn insert(&mut self, record: Record, transaction: Option<&dyn Transaction>) {
        self.map.insert(record.key, record.value);
    }

    fn remove(&mut self, record: Record, transaction: Option<&dyn Transaction>) {
        unimplemented!()
    }
}


pub fn foobar() -> String {
    String::from("foobar")
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::sync::{Mutex, Arc};
    use std::borrow::BorrowMut;

    #[test]
    fn it_works() {


        let db_orig = Arc::new(Mutex::new(SimpleMemDB::new()));

        let db = Arc::clone(&db_orig);
        let handle = thread::spawn(move || {
            let mut db_unlocked = db.lock().unwrap();
            db_unlocked.create_index("foo", Key::Int(0)).unwrap();
            let i = db_unlocked.open_index("foo").unwrap();
            i.insert(Record {key: Key::Int(0), value: vec![0; 101]}, None);

            assert_eq!(i.get_single(Key::Int(0), None), Record {key: Key::Int(0), value: vec![0; 101]});
        });
        handle.join();
        let db = Arc::clone(&db_orig);
        let handle2 = thread::spawn(move || {
            let mut db_unlocked = db.lock().unwrap();


            let i = db_unlocked.open_index("foo").unwrap();
            i.insert(Record {key: Key::Int(0), value: vec![0; 101]}, None);

            assert_eq!(i.get_single(Key::Int(0), None), Record {key: Key::Int(0), value: vec![0; 101]});
        });
        handle2.join();


    }
}
