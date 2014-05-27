use super::{
    Field,
    FieldSchema,
    RewindableIterator,
    TableIterator,
    TableSchema,
};

pub struct Select<'closure, Iter> {
    pub base: Iter,
    pub condition: |&Vec<Field>|:'closure -> bool,
}

impl<'closure, Iter: TableIterator> Iterator<Vec<Field>> for Select<'closure, Iter> {
    fn next(&mut self) -> Option<Vec<Field>> {
        loop {
            match self.base.next() {
                None => return None,
                Some(val) =>
                    if (self.condition)(&val) { return Some(val) }
                    else { continue; }
            }
        }
    }
}

impl<'closure, Iter: TableIterator> TableIterator for Select<'closure, Iter> {
    fn blocks_accessed(&self) -> uint {
        self.base.blocks_accessed()
    }

    fn records_accessed(&self) -> uint {
        self.base.records_accessed()
    }

    fn schema<'s>(&'s self) -> &'s TableSchema {
        self.base.schema()
    }
}

pub struct CrossJoin<IterA, IterB> {
    iter_a: IterA,
    iter_b: IterB,

    current_a: Option<Vec<Field>>,
    schema: TableSchema,
}

pub fn cross<
    IterA: TableIterator,
    IterB: TableIterator + RewindableIterator<Vec<Field>>
>(mut iter_a: IterA, iter_b: IterB) -> CrossJoin<IterA, IterB> {
    let schema = {
        let sa = iter_a.schema();
        let sb = iter_b.schema();

        let mut fields = Vec::with_capacity(sa.fields.len() + sb.fields.len());
        fields.extend(sa.fields.iter().map(|f| FieldSchema {
            name: format!("{}.{}", sa.name, f.name), ..*f
        }));
        fields.extend(sb.fields.iter().map(|f| FieldSchema {
            name: format!("{}.{}", sb.name, f.name),
            offset: f.offset + sa.entry_stride, ..*f
        }));

        TableSchema {
            name: "cross-join".to_owned(),
            fields: fields,
            entry_stride: sa.entry_stride + sb.entry_stride,
        }
    };

    let first_a = iter_a.next();

    CrossJoin {
        iter_a: iter_a,
        iter_b: iter_b,

        current_a: first_a,
        schema: schema,
    }
}

impl<
    IterA: TableIterator,
    IterB: TableIterator + RewindableIterator<Vec<Field>>
> TableIterator for CrossJoin<IterA, IterB> {
    fn blocks_accessed(&self) -> uint {
        self.iter_a.blocks_accessed() + self.iter_b.blocks_accessed()
    }

    fn records_accessed(&self) -> uint {
        self.iter_a.records_accessed() + self.iter_b.records_accessed()
    }

    fn schema<'s>(&'s self) -> &'s TableSchema {
        &self.schema
    }
}

impl<
    IterA: TableIterator,
    IterB: TableIterator + RewindableIterator<Vec<Field>>
> Iterator<Vec<Field>> for CrossJoin<IterA, IterB> {
    fn next(&mut self) -> Option<Vec<Field>> {
        loop {
            return match self.current_a {
                None => None,
                Some(_) => match self.iter_b.next() {
                    None => {
                        self.current_a = self.iter_a.next();
                        self.iter_b.rewind();
                        continue;
                    },
                    Some(b) => Some(self.current_a.get_ref() + b),
                },
            };
        }
    }
}
