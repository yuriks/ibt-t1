BINS := table-gen dump-tables
LIBS := db

DEPENDS_table-gen := db
DEPENDS_dump-tables := db

RUST_FLAGS := -g

include rust.mk
