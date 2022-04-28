use super::*;
use std::fs;
use std::path::PathBuf;

pub fn open_file(path: &PathBuf) -> RecordFileResult<direct_io::File> {
    file_manager::get_file_manager()
        .open_file_with(
            path.clone(),
            &fs::OpenOptions::new().read(true).write(true).create(true),
        )
        .map_err(|err| RecordFileError::OpenFile { source: err })
}