extern crate db;

use std::io::stdio::println;

fn main() {
    let db_path = Path::new("empresa.db");

    let mut depts = db::Table::open(&db_path, "Departamentos").unwrap();

    let mut dept_iter = depts.iter();
    loop {
        let mut values = Vec::new();
        if !dept_iter.next_record(&mut values) {
            break;
        }
        let arr : Vec<String> = values.iter().map(|&x| format!("{}", x)).collect();
        println(arr.connect("\t").as_slice());
    }

    let mut clients = db::Table::open(&db_path, "Clientes").unwrap();

    let mut client_iter = clients.iter();
    loop {
        let mut values = Vec::new();
        if !client_iter.next_record(&mut values) {
            break;
        }
        let arr : Vec<String> = values.iter().map(|&x| format!("{}", x)).collect();
        println(arr.connect("\t").as_slice());
    }
}
