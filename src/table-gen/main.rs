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

    let names = [
        "Fulano", "João", "Yuri", "Hugo", "Maria", "Sandra", "Alexandre", "Ricardo", "Ciclano",
        "Beltrano", "Davi", "Luís", "Jacob", "da Silva", "Kunde", "Roberts", "Denny", "Eacret",
        "Doug", "Gulbranson", "Alina", "Hargraves", "Elva", "Niblett", "Harriet", "Ornelas",
        "Leida", "Stackhouse", "Harold", "Gibson", "Velma", "Pless", "Milford", "Lymon", "Danae",
        "Humfeld", "Jamee", "Truesdell", "Melita", "Hunsucker", "Nieves", "Bish", "Meghan",
        "Fritze", "Laronda", "Byrd", "Simonne", "Friel", "Jule", "Dade", "Hester", "Roesler",
        "Shameka", "Brim", "Jefferey", "Mcneely", "Brittaney", "Mullikin", "Casandra", "Washam",
        "Tessa", "Nordstrom", "Jennifer", "Wilmes", "Harris", "Henze", "Krystle", "Vice", "Ollie",
        "Laird", "Colby", "Aylesworth", "Gilberto", "Colon", "Arie", "Brodt", "Esperanza", "Huskey",
        "Tyson", "Viruet", "Letitia", "Dresser", "Jaimie", "Cupples", "Alethea", "Arline",
        "Dorthea", "Bolinger", "Manual", "Cartee", "Fabiola", "Nolan", "Genny", "Vaughan", "Hana",
        "Difranco", "Frederick", "Hollinger", "Louie", "Dalal", "Thaddeus", "Ptak", "Joey",
        "Pennock", "Gregory", "Belliveau", "Peter", "Bueche", "Melonie", "Caves", "Laverne", "Yoo",
        "Coreen", "Barmore", "Sulema", "Branton", "Torri", "Kelsey", "Denis", "Paille", "Teressa",
        "Decosta", "Juliana", "Perrin", "Eleanora", "Atherton", "Trudy", "Mcgahey", "Zaida",
        "Camacho", "Anamaria", "Eves", "Kate", "Tardy", "Vicenta", "Olive", "Rickie", "Drury",
        "Kassandra", "Mulvey", "Kristle", "Street", "Nestor", "Lien", "Addie", "Keesler", "Sharan",
        "Groat", "Lionel", "Hindle", "Nichelle", "Santamaria", "Nannette", "Austell", "Dalila",
        "Burdick", "Reyes", "Seiber", "Orville", "Daniele", "Rigoberto", "Urman", "Armand",
        "Powell", "Son", "Kane", "Vito", "Vila", "Martina", "Heatherly", "Jolynn", "Decoteau",
        "Junita", "Scheck", "Sid", "Garneau", "Mildred", "Callihan", "Suzan", "Kosakowski",
        "Christine", "Spath", "Shari", "Schaar", "Bonnie", "Luby", "Aliza", "Beckerman", "Nancie",
        "Tavarez", "Philomena", "Knauss", "Yasmine", "Steve", "Sarita", "Terrell", "Mariela",
        "Sack", "Jeff", "Vanderslice", "Lorri", "Clayborn", "Tatum", "Weigle", "Ettie", "Raiford",
        "Susanne", "Whitner", "Del", "Gregorich", "Brandon", "Sapien", "Lavon", "Bump", "Margrett",
        "Vu", "Ruthe", "Giardina", "Tracee", "Stapp", "Richelle", "Emberton", "Yuri", "Marois",
        "Stephine", "Morphis", "Roscoe", "Poissant", "Lilly", "Wachtel", "Laure", "Genova", "Sadie",
        "Wallach", "Rodolfo", "Merritt", "Terresa", "Steelman", "Marchelle", "Kelso", "Valery",
        "Hagstrom", "Leda", "Peri", "Karole", "Vangorder", "Shawana", "Monaco", "Mallory",
        "Tomasello", "Jeffie", "Mccuen", "Roseanne", "Isom", "Rheba", "Alpers", "Jessenia",
        "Chiesa", "Gwyneth", "Mcfetridge", "Twanna", "Mckinsey", "Morris", "Tietz", "Angelo",
        "Yung", "Janean", "Frady", "Marhta", "Begay", "Elijah", "Goncalves", "Adele", "Leno",
        "Jannette", "Marland", "Hank", "Cypher", "Madge", "Dale", "Rosenda", "Severe", "Mariel",
        "Kaneshiro", "Yer", "Bensinger", "Myesha", "Bogard", "Cary", "Witter", "Piedad", "August",
        "Shea", "Blosser", "Evangelina", "Hornbuckle", "Minerva", "Kolman", "Cassidy", "Hardaway",
        "Tyrone", "Varnum", "Aldo", "Thurmon", "Catherina", "Croyle", "Enoch", "Tokar", "Gala",
        "Strack", "Lucilla", "Lynch", "Delicia", "Knobel", "Lala", "Porter"];
    let dept_names = ["Punheta", "Soneca", "Vendas"];

    {
        let mut depts = db::Table::open(&db_path, "Departamentos").unwrap();

        for (i, dept) in dept_names.iter().enumerate() {
            let entry = [db::Integer(i as u32),
                         db::Text(dept.into_owned())];
            let foo = depts.append_entry(entry.as_slice());
            foo.unwrap();
        }
    }

    {
        let mut clients = db::Table::open(&db_path, "Clientes").unwrap();

        let mut rng = rand::task_rng();
        let dept_sampler = Range::new(0, dept_names.len());

        let mut next_id = 0;

        for first in names.iter() {
            for last in names.iter() {
                let full_name = format!("{} {}", first, last);
                let dept_id = dept_sampler.ind_sample(&mut rng);
                let entry = [db::Integer(next_id),
                             db::Text(full_name),
                             db::Integer(dept_id as u32)];
                clients.append_entry(entry.as_slice()).unwrap();
                next_id += 1;
            }
        }
    }
}
