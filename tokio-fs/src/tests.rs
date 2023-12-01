use std::sync::Arc;

use dmds::{world, World};

use crate::{FsHandle, ShutdownHandle};

#[test]
fn shutdown_handle() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let world: Arc<World<[u64; 2], 2, _>> = Arc::new(world! {
            FsHandle::new(".test/fs_save", false) => 64 | ..1024, 64 | ..1024
        });
        let _sh = ShutdownHandle::new(world.clone());

        assert!(world.insert([114, 514]).await.is_ok());
        drop(_sh);

        let pos = world.chunk_pos_of_data(&[114, 514]).unwrap();
        assert!(
            tokio::fs::File::open(format!(".test/fs_save/{}/{}", pos[0], pos[1]))
                .await
                .is_ok()
        );
        assert!(tokio::fs::remove_dir_all(".test/fs_save").await.is_ok());
    })
}
