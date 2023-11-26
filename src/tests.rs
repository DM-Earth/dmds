use futures_lite::future::block_on;

use crate::{mem_io_handle::MemStorage, world::Dim, World};

#[test]
fn insert() {
    block_on(async {
        let world: World<[u64; 2], 2, _> = World::new(
            [
                Dim {
                    range: ..1024,
                    items_per_chunk: 8,
                },
                Dim {
                    range: ..1024,
                    items_per_chunk: 16,
                },
            ],
            MemStorage::new(),
        );

        assert!(world.insert([114, 514]).await.is_ok());

        let w_read = world.chunks_buf.iter().next().unwrap();
        let chunk_read = w_read.data.read().await;

        let (id, val) = chunk_read.get(0).unwrap();

        assert_eq!(*id, 114);
        assert_eq!([*id, 514], *val.read().await);
    })
}

#[test]
fn remove() {
    block_on(async {
        let world: World<[u64; 2], 2, _> = World::new(
            [
                Dim {
                    range: ..1024,
                    items_per_chunk: 8,
                },
                Dim {
                    range: ..1024,
                    items_per_chunk: 16,
                },
            ],
            MemStorage::new(),
        );

        assert!(world.insert([114, 514]).await.is_ok());

        assert_eq!(
            world
                .chunk_buf_of_data_or_load(&[114, 514])
                .await
                .unwrap()
                .remove(114)
                .await,
            Some([114, 514])
        );

        let w_read = world.chunks_buf.iter().next().unwrap();
        let chunk_read = w_read.data.read().await;

        assert!(chunk_read.get(0).is_none());
    })
}
