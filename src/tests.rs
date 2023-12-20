use std::sync::Arc;

use bytes::BytesMut;
use futures_lite::{future::block_on, AsyncWriteExt, StreamExt};

use crate::{mem_io_handle::MemStorage, world::Dim, World};

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

        let val = chunk_read.get(&114).unwrap();
        assert_eq!(Some([114, 514]), *val.read().await);
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

        assert!(chunk_read.is_empty());
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
                .write_chunk(crate::ARRAY_VERSION, pos)
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

            let chunk = world.chunk_buf_of_pos_or_load(pos).await.unwrap();
            let data = chunk.data.read().await;
            assert_eq!(
                *data.iter().next().unwrap().1.read().await,
                Some([114, 514])
            );
        }
    })
}

#[test]
fn select() {
    block_on(async {
        let world: World<[u64; 2], 2, _> = World::new(
            [
                Dim {
                    range: ..100,
                    items_per_chunk: 25,
                },
                Dim {
                    range: ..100,
                    items_per_chunk: 25,
                },
            ],
            MemStorage::new(),
        );

        assert!(world.insert([10, 39]).await.is_ok());
        assert!(world.insert([99, 20]).await.is_ok());

        let selection = world.select_all();
        let mut iter = selection.iter();

        let a = *iter.next().await.unwrap().unwrap().get().await.unwrap();

        match a {
            [10, 39] => {
                assert_eq!(
                    iter.next().await.unwrap().unwrap().get().await.unwrap(),
                    &[99, 20]
                );
            }
            [99, 20] => {
                assert_eq!(
                    iter.next().await.unwrap().unwrap().get().await.unwrap(),
                    &[10, 39]
                );
            }
            _ => unreachable!(),
        }
    })
}

#[test]
fn io_modify() {
    block_on(async {
        let mem = Arc::new(MemStorage::new());
        let pos;

        {
            let world: World<[u64; 2], 2, _> = World::new(
                [
                    Dim {
                        range: ..1024,
                        items_per_chunk: 128,
                    },
                    Dim {
                        range: ..1024,
                        items_per_chunk: 64,
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
                .write_chunk(crate::ARRAY_VERSION, pos)
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
                        items_per_chunk: 128,
                    },
                    Dim {
                        range: ..1024,
                        items_per_chunk: 64,
                    },
                ],
                mem.clone(),
            );

            let selection = world.select_all();
            let mut iter = selection.iter();

            let mut lazy = iter.next().await.unwrap().unwrap();

            lazy.get_mut().await.unwrap()[1] = 810;
            assert_eq!(lazy.get().await.unwrap()[1], 810);
        }
    })
}

#[test]
fn buf_modify() {
    block_on(async {
        let world: World<[u64; 2], 2, _> = World::new(
            [
                Dim {
                    range: ..1024,
                    items_per_chunk: 32,
                },
                Dim {
                    range: ..1024,
                    items_per_chunk: 32,
                },
            ],
            MemStorage::new(),
        );

        assert!(world.insert([114, 514]).await.is_ok());

        let selection = world.select_all();
        let mut iter = selection.iter();

        let mut lazy = iter.next().await.unwrap().unwrap();

        lazy.get_mut().await.unwrap()[1] = 810;
        assert_eq!(lazy.get().await.unwrap()[1], 810);
    })
}
