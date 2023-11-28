use std::sync::Arc;

use bytes::BytesMut;
use futures_lite::{future::block_on, AsyncWriteExt};

use crate::{mem_io_handle::MemStorage, world::Dim, IoHandle, World};

#[test]
fn buf_insert() {
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
fn buf_remove() {
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

#[test]
fn buf_save_load() {
    block_on(async {
        let mem = Arc::new(MemStorage::new());
        let pos;

        {
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
                mem.clone(),
            );

            assert!(world.insert([114, 514]).await.is_ok());

            let w_read = world.chunks_buf.iter().next().unwrap();
            pos = world.chunk_pos_of_data(&[114, 514]).unwrap();

            let mut bytes = BytesMut::new();
            assert!(w_read.write_buf(&mut bytes).await.is_ok());

            assert!(mem
                .write_chunk(pos)
                .await
                .unwrap()
                .write_all(&bytes)
                .await
                .is_ok());
        }

        {
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
                mem.clone(),
            );

            //FIXME: stream blocked
            let chunk = world.chunk_buf_of_pos_or_load(pos).await.unwrap();
            let data = chunk.data.read().await;
            assert_eq!(*data.iter().next().unwrap().1.read().await, [114, 514]);
        }
    })
}
