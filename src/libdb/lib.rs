#![crate_id="github.com/yuriks/ibt-t1/db"]
#![crate_type="lib"]

extern crate core;
extern crate serialize;

use std::fmt;
use std::io;
use std::io::fs;
use std::str;
use core::slice::MutableCloneableVector;
use serialize::json;
use serialize::{
    Encodable,
    Decodable,
    Encoder
};

#[deriving(Decodable, Encodable, Eq, Show)]
pub enum FieldType {
    IntegerType,
    TextType,
}

pub enum Field<'a> {
    Integer(u32),
    Text(&'a str),
}

impl<'a> Field<'a> {
    fn get_type(&self) -> FieldType {
        match *self {
            Integer(_) => IntegerType,
            Text(_) => TextType,
        }
    }
}

impl<'a> fmt::Show for Field<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Integer(x) => write!(fmt, "{}", x),
            Text(ref s) => write!(fmt, "{}", s),
        }
    }
}


#[deriving(Decodable, Encodable)]
pub struct FieldSchema {
    pub name: String,
    pub offset: uint,
    pub data_type: FieldType,
    pub length: uint,
}

#[deriving(Decodable, Encodable)]
pub struct TableSchema {
    pub name: String,
    pub fields: Vec<FieldSchema>,
    pub entry_stride: uint,
}

pub struct Table {
    pub schema: TableSchema,
    pub file: fs::File,
}

pub struct TableIterator<'table> {
    table: &'table mut Table,
    i: uint,
    len: uint,

    block_base: Option<(uint, uint)>,
    block_data: Vec<u8>,
}

static BLOCK_SIZE : uint = 10;

impl<'s, 'table> TableIterator<'table> {
    fn load_block(&mut self, i: uint) -> io::IoResult<()> {
        let stride = self.table.schema.entry_stride;
        let load_base = (i * stride) as i64;
        let load_size = BLOCK_SIZE * stride;

        self.block_base = None;
        self.block_data.clear();
        try!(self.table.file.seek(load_base, io::SeekSet));
        let bytes_loaded = try!(self.table.file.push(load_size, &mut self.block_data));
        let records_loaded = bytes_loaded / stride;
        assert!(records_loaded >= 1);
        self.block_base = Some((i, i + records_loaded));

        Ok(())
    }

    pub fn next_record(&'s mut self, out: &mut Vec<Field<'s>>) -> bool {
        if self.i >= self.len {
            return false;
        }

        let stride = self.table.schema.entry_stride;

        let need_reload = match self.block_base {
            Some((base, limit)) => self.i < base || self.i >= limit,
            None => true
        };
        if need_reload {
            self.load_block(self.i).unwrap();
        }

        let (base, _) = self.block_base.unwrap();

        let entry_base = (self.i - base) * stride;
        let entry_buf = self.block_data.slice(entry_base, entry_base + stride);

        read_fields(out, self.table.schema.fields.as_slice(), entry_buf).unwrap();
        self.i += 1;

        true
    }
}

fn read_u32(buf: &[u8]) -> u32 {
    buf[0] as u32 << 24 |
    buf[1] as u32 << 16 |
    buf[2] as u32 <<  8 |
    buf[3] as u32
}

fn write_u32(val: u32, buf: &mut [u8]) {
    buf[0] = (val >> 24) as u8;
    buf[1] = (val >> 16) as u8;
    buf[2] = (val >>  8) as u8;
    buf[3] = (val >>  0) as u8;
}

#[deriving(Show)]
pub enum TableOpenError {
    OpenIoError(io::IoError),
    ParserError(json::ParserError),
    DecoderError(json::DecoderError),
}

pub enum TableError {
    IoError(io::IoError),
    TypeError(uint, FieldType, FieldType), // (index, actual, expected)
    LengthError(uint, uint, uint), // (index, actual, expected)
    ValueError(uint),
}

impl fmt::Show for TableError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            IoError(ref e) => e.fmt(fmt),
            TypeError(index, actual, expected) => write!(fmt,
                    "Field {} has incorrect type. Expected {} but got {}.",
                    index, expected, actual),
            LengthError(index, actual, expected) => write!(fmt,
                    "Field {} has incorrect length. Expected {} but got {}.",
                    index, expected, actual),
            ValueError(index) => write!(fmt,
                    "Field {} contains invalid data.", index),
        }
    }
}

fn read_value<'a>(i: uint, field_type: FieldType, buf: &'a [u8]) -> Result<Field<'a>, TableError> {
    match field_type {
        IntegerType => {
            Ok(Integer(read_u32(buf)))
        },
        TextType => {
            let len = buf[0] as uint;
            let s = str::from_utf8(buf.slice(1, 1 + len));
            match s {
                Some(s) => Ok(Text(s)),
                None => Err(ValueError(i)),
            }
        },
    }
}

fn write_value(i: uint, value: &Field, buf: &mut [u8]) -> Result<(), TableError> {
    match *value {
        Integer(x) => {
            write_u32(x, buf);
            Ok(())
        },
        Text(ref s) => {
            if s.len() > 255 {
                return Err(LengthError(i, s.len(), 255));
            }
            buf[0] = s.len() as u8;
            buf.mut_slice_from(1).copy_from(s.as_bytes());
            Ok(())
        },
    }
}

fn read_fields<'a>(values: &mut Vec<Field<'a>>, fields: &[FieldSchema], buffer: &'a [u8])
        -> Result<(), TableError> {
    values.clear();
    values.reserve(fields.len());

    for (i, field) in fields.iter().enumerate() {
        let field_buf = buffer.slice(field.offset, field.offset + field.length);
        values.push(try!(read_value(i, field.data_type, field_buf)));
    }

    Ok(())
}

fn write_fields(values: &[Field], fields: &[FieldSchema], buffer: &mut [u8]) -> Result<(), TableError> {
    for (i, (value, field)) in values.iter().zip(fields.iter()).enumerate() {
        let field_buf = buffer.mut_slice(field.offset, field.offset + field.length);
        if value.get_type() != field.data_type {
            return Err(TypeError(i, value.get_type(), field.data_type));
        }
        try!(write_value(i, value, field_buf));
    }

    Ok(())
}

impl Table {
    pub fn open<'a>(db_path: &Path, table_name: &'a str) -> Result<Table, TableOpenError> {
        let table_path = db_path.join("tables").join(table_name);

        let data_file = match fs::File::open_mode(
                &table_path.join("data.bin"), io::Open, io::ReadWrite) {
            Ok(f) => f, Err(e) => return Err(OpenIoError(e)) };
        let mut schema_file = match fs::File::open(&table_path.join("schema.json")) {
            Ok(f) => f, Err(e) => return Err(OpenIoError(e)) };

        let schema_json = match json::from_reader(&mut schema_file) {
            Ok(j) => j, Err(e) => return Err(ParserError(e)) };
        let schema = match Decodable::decode(&mut json::Decoder::new(schema_json)) {
            Ok(s) => s, Err(e) => return Err(DecoderError(e)) };

        Ok(Table { schema: schema, file: data_file })
    }

    pub fn iter<'s>(&'s mut self) -> TableIterator<'s> {
        let num_entries = self.file.stat().unwrap().size / self.schema.entry_stride as u64;
        TableIterator {
            table: self,
            i: 0,
            len: num_entries as uint,

            block_base: None,
            block_data: Vec::new(),
        }
    }

    pub fn append_entry<'a>(&mut self, values: &[Field]) -> Result<(), TableError> {
        let mut buffer = Vec::from_elem(self.schema.entry_stride, 0u8);

        try!(write_fields(values, self.schema.fields.as_slice(), buffer.as_mut_slice()));

        match self.file.seek(0, io::SeekEnd) {
            Ok(()) => (), Err(e) => return Err(IoError(e)) };
        match self.file.write(buffer.as_slice()) {
            Ok(()) => (), Err(e) => return Err(IoError(e)) };

        Ok(())
    }
}

pub fn validate_schema(schema: &TableSchema) -> Result<(), String> {
    let mut used_bytes = Vec::from_elem(schema.entry_stride, false);

    for field in schema.fields.iter() {
        // Ensure field is inside entry.
        if field.offset + field.length > used_bytes.len() {
            return Err(format!("Field `{}`'s offset exceeds entry size.", field.name));
        }

        // Ensure field's length is valid acoording to type.
        match field.data_type {
            IntegerType => {
                if field.length != 4 {
                    return Err(format!(
                            "Field `{}` is Integer and must have length 4.", field.name));
                }
            },
            TextType => {
                if field.length > 256 {
                    return Err(format!(
                            "Field `{}` is Text and must have length of at most 256.", field.name));
                }
            },
        }

        // Ensure field doesn't overlap other fields.
        let field_slice = used_bytes.mut_slice(field.offset, field.offset + field.length);
        for pos in field_slice.mut_iter() {
            if *pos {
                return Err(format!("Field `{}` overlaps another field.", field.name));
            }
            *pos = true;
        }
    }

    Ok(())
}

pub fn create_table(db_path: &Path, schema: &TableSchema) -> io::IoResult<()> {
    let table_path = db_path.join("tables").join(schema.name.as_slice());
    try!(fs::mkdir_recursive(&table_path, io::UserDir));

    try!(fs::File::create(&table_path.join("data.bin")));

    let mut schema_file = try!(fs::File::create(&table_path.join("schema.json")));
    try!(schema.encode(&mut json::Encoder::new(&mut schema_file)));

    Ok(())
}