# The MIT License (MIT)
#
# Copyright (c) 2014 Yuri Kunde Schlesner
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

RUSTC ?= rustc
RUST_FLAGS ?= -O
CREATE_LINKS ?= 1

RUST_TARGET := $(shell $(RUSTC) --version | sed -ne 's/^host:\s*//p')

$(foreach lib,$(LIBS),\
	$(eval LIB_$(lib) := $(shell $(RUSTC) --crate-file-name src/lib$(lib)/lib.rs)))

OUTPUT_DIR := target/$(RUST_TARGET)
OUTPUT_BINS := $(foreach bin,$(BINS),$(OUTPUT_DIR)/$(bin))
OUTPUT_LIBS := $(foreach lib,$(LIBS),$(OUTPUT_DIR)/$(LIB_$(lib)))

.PHONY : all
all : bins libs

.PHONY : bins
bins : $(OUTPUT_BINS)

.PHONY : libs
libs : $(OUTPUT_LIBS)

.PHONY : clean
clean :
	rm -rf target/
ifeq ($(CREATE_LINKS),1)
	rm -rf $(BINS)
endif

define make-lib=
$$(OUTPUT_DIR)/$$(LIB_$1) : src/lib$1/lib.rs $$(foreach lib,$$(DEPENDS_lib$1),$$(OUTPUT_DIR)/$$(LIB_$$(lib)))
	@mkdir -p $$(dir $$@) $$(OUTPUT_DIR)/depends/
	$$(RUSTC) $$(RUST_FLAGS) $$< \
		-L $$(OUTPUT_DIR) \
		--dep-info $$(OUTPUT_DIR)/depends/$$(notdir $$@).d \
		--out-dir $$(dir $$@)
endef

define make-bin=
$$(OUTPUT_DIR)/$1 : src/$1/main.rs $$(foreach lib,$$(DEPENDS_$1),$$(OUTPUT_DIR)/$$(LIB_$$(lib)))
	@mkdir -p $$(dir $$@) $$(OUTPUT_DIR)/depends/
	$$(RUSTC) $$(RUST_FLAGS) $$< \
		-L $$(OUTPUT_DIR) \
		--dep-info $$(OUTPUT_DIR)/depends/$$(notdir $$@).d \
		-o $$@
ifeq ($(CREATE_LINKS),1)
	@ln -sf $$@
endif
endef

-include $(wildcard $(OUTPUT_DIR)/depends/*.d)

$(foreach lib,$(LIBS),\
	$(eval $(call make-lib,$(lib))))
$(foreach bin,$(BINS),\
	$(eval $(call make-bin,$(bin))))
