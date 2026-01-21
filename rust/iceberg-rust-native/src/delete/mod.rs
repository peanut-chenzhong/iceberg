//! Delete file indexing and lookup
//!
//! This module provides efficient indexing and lookup of delete files for data files.
//! It supports:
//! - Position deletes (by partition and by path)
//! - Equality deletes (global and by partition)
//! - Deletion Vectors (DVs)
//! - Sequence number based filtering

mod index;
mod position;
mod equality;

pub use index::{DeleteFileIndex, DeleteFileIndexBuilder};
pub use position::PositionDeletes;
pub use equality::EqualityDeletes;
