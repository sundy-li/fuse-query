// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::fmt;
use std::fmt::Display;

use crate::processors::{EmptyProcessor, MergeProcessor, Pipeline};

impl Pipeline {
    pub fn display_indent(&self) -> impl fmt::Display + '_ {
        struct Wrapper<'a>(&'a Pipeline);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let mut indent = 0;
                let mut write_indent = |f: &mut fmt::Formatter| -> fmt::Result {
                    if indent > 0 {
                        writeln!(f)?;
                    }
                    for _ in 0..indent {
                        write!(f, "  ")?;
                    }
                    indent += 1;
                    Ok(())
                };

                let mut index = 0;
                self.0
                    .walk_preorder(|pipe| {
                        write_indent(f)?;

                        let ways = pipe.nums();
                        let processor = pipe.processor_by_index(0);

                        processor_match_downcast!(processor, {
                        empty:EmptyProcessor  => write!(f, "")?,
                        merge:MergeProcessor => {
                            let prev_pipe = self.0.pipe_by_index(index);
                            let prev_name = prev_pipe.processor_by_index(0).name().to_string();
                            let prev_ways = prev_pipe.nums();

                            let post_pipe = self.0.pipe_by_index(index + 2);
                            let post_name = post_pipe.processor_by_index(0).name().to_string();
                            let post_ways = post_pipe.nums();

                            write!(
                                f,
                                "Merge ({} × {} {}) to ({} × {})",
                                prev_name,
                                prev_ways,
                                if prev_ways == 1 {
                                    "processor"
                                } else {
                                    "processors"
                                },
                                post_name,
                                post_ways,
                            )?;
                        },
                        _=> {
                             write!(
                                f,
                                "{} × {} {}",
                                processor.name(),
                                ways,
                                if ways == 1 { "processor" } else { "processors" },
                            )?;
                        }
                        });
                        index += 1;
                        Ok(true)
                    })
                    .map_err(|_| fmt::Error)?;
                Ok(())
            }
        }
        Wrapper(self)
    }

    pub fn display_graphviz(&self) -> impl fmt::Display + '_ {
        struct Wrapper<'a>(&'a Pipeline);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                writeln!(
                    f,
                    "// Begin DataFuse GraphViz Pipeline (see https://graphviz.org)"
                )?;
                writeln!(f, "digraph {{")?;
                // TODO()
                writeln!(f, "}}")?;
                writeln!(f, "// End DataFuse GraphViz Pipeline")?;
                Ok(())
            }
        }
        Wrapper(self)
    }
}

impl fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.display_indent().fmt(f)
    }
}