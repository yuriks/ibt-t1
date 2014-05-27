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
    println!("Blocks accessed: {}, Records accessed: {}\n",
             it.blocks_accessed(), it.records_accessed());
}

fn main() {
    let db_path = Path::new("empresa.db");
    let mut depts = db::Table::open(&db_path, "Departamentos").unwrap();
    let mut clients = db::Table::open(&db_path, "Clientes").unwrap();

    print_table(&mut depts.iter());
    //print_table(&mut clients.iter());

    /*
    let mut pk_iter = db::select::SelectPrimaryKey {
        base: clients.iter(),
        key: Some(99820),
    };
    print_table(&mut pk_iter);
    */

    let mut cross_iter = db::select::cross(clients.iter(), depts.iter());
    let client_id_field = cross_iter.schema().map_field("Clientes.departamento").unwrap();
    let dept_id_field = cross_iter.schema().map_field("Departamentos.id").unwrap();
    let mut select_iter = db::select::Select {
        base: cross_iter,
        condition: |record| { record.get(client_id_field) == record.get(dept_id_field) },
    };
    print_table(&mut select_iter);

    /*
    let clients_iter = clients.iter();
    let client_id_field = clients_iter.schema().map_field("departamento").unwrap();
    let mut pk_join_iter = db::select::pk_join(clients_iter, depts.iter(),
        |record| { match *record.get(client_id_field) {
            db::Integer(k) => Some(k),
            _ => None,
        }});
    print_table(&mut pk_join_iter);
    */
}
