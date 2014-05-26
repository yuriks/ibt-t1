extern crate serialize;
extern crate db;
extern crate rand;

use rand::distributions::{
    Range,
    IndependentSample,
};

fn create_tables(db_path: &Path) {
    let depts_schema = db::TableSchema {
        name: "Departamentos".into_strbuf(),
        fields: vec![
            db::FieldSchema {
                name: "id".into_strbuf(),
                offset: 0,
                data_type: db::IntegerType,
                length: 4,
            },
            db::FieldSchema {
                name: "nome".into_strbuf(),
                offset: 4,
                data_type: db::TextType,
                length: 20,
            }],
        entry_stride: 24,
    };

    let clients_schema = db::TableSchema {
        name: "Clientes".into_strbuf(),
        fields: vec![
            db::FieldSchema {
                name: "id".into_strbuf(),
                offset: 0,
                data_type: db::IntegerType,
                length: 4,
            },
            db::FieldSchema {
                name: "nome".into_strbuf(),
                offset: 8,
                data_type: db::TextType,
                length: 20,
            },
            db::FieldSchema {
                name: "departamento".into_strbuf(),
                offset: 4,
                data_type: db::IntegerType,
                length: 4,
            }],
        entry_stride: 28,
    };

    db::validate_schema(&clients_schema).unwrap();
    db::validate_schema(&depts_schema).unwrap();
    db::create_table(db_path, &depts_schema).unwrap();
    db::create_table(db_path, &clients_schema).unwrap();
}

fn main() {
    let db_path = Path::new("empresa.db");

    create_tables(&db_path);

    let first_names = ["Fulano", "João", "Yuri", "Hugo"];
    let last_names = ["da Silva", "Kunde", "Roberts"];
    let dept_names = ["Punheta", "Soneca", "Vendas"];

    {
        let mut depts = db::Table::open(&db_path, "Departamentos").unwrap();

        for (i, dept) in dept_names.iter().enumerate() {
            let entry = [db::Integer(i as u32),
                         db::Text(*dept)];
            let foo = depts.append_entry(entry.as_slice());
            foo.unwrap();
        }
    }

    {
        let mut clients = db::Table::open(&db_path, "Clientes").unwrap();

        let mut rng = rand::task_rng();
        let dept_sampler = Range::new(0, dept_names.len());

        let mut next_id = 0;

        for first in first_names.iter() {
            for last in last_names.iter() {
                let full_name = format!("{} {}", first, last);
                let dept_id = dept_sampler.ind_sample(&mut rng);
                let entry = [db::Integer(next_id),
                             db::Text(full_name.as_slice()),
                             db::Integer(dept_id as u32)];
                clients.append_entry(entry.as_slice()).unwrap();
                next_id += 1;
            }
        }
    }
}
