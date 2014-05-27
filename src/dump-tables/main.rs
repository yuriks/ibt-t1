extern crate db;

use db::TableIterator;

use std::io::stdio::println;

fn print_table_header(schema: &db::TableSchema) {
    let arr : Vec<String> = schema.fields.iter().map(|ref x| format!("{}", x.name)).collect();
    println(arr.connect("\t").as_slice());
    println("-----------------------------------------------------");
}

fn print_table<Iter: db::TableIterator>(it: &mut Iter) {
    print_table_header(it.schema());
    for mut values in *it {
        let arr : Vec<String> = values.mut_iter().map(|x| format!("{}", x)).collect();
        println(arr.connect("\t").as_slice());
    }
    println!("Blocks accessed: {}, Records accessed: {}\n", it.blocks_accessed(), it.records_accessed());
}

fn main() {
    let db_path = Path::new("empresa.db");
    let mut depts = db::Table::open(&db_path, "Departamentos").unwrap();
    let mut clients = db::Table::open(&db_path, "Clientes").unwrap();

    print_table(&mut depts.iter());

    let clients_iter = clients.iter();
    let id_field = clients_iter.schema().map_field("id").unwrap();
    let mut select = db::select::Select {
        base: clients_iter,
        condition: |record| {
            match *record.get(id_field) {
                db::Integer(x) => x == 5,
                _ => fail!("Unexpected type"),
            }
        },
    };
    print_table(&mut select);
}
