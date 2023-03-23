use std::{collections::HashMap, sync::mpsc::sync_channel, io::Write};

use clap::Parser;

use serde::{Serialize, Deserialize};

#[derive(Parser, Debug)]
struct Config {
    csv_dir: String,
    output_file: String,
}

type VertexCardinality = HashMap<String, f64>;
type EdgeCardinality =
    HashMap<String, HashMap<String, HashMap<String, f64>>>;

#[derive(Debug, Default, Serialize, Deserialize)]
struct Statistics {
    vertex_cardinality: VertexCardinality,
    edge_cardinality: EdgeCardinality,
}

#[derive(Debug, Default)]
struct Context {
    // vertex_cardinality: VertexCardinality,
    // edge_cardinality: EdgeCardinality,
    statistics: Statistics,

    place: HashMap<u64, String>,
    organisation: HashMap<u64, String>,
}

impl Context {
    async fn import_vertex(&mut self, path: std::path::PathBuf, label_name: String) {
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .delimiter(b'|')
            .from_path(path)
            .unwrap();

        let header: Vec<String> = rdr.headers().unwrap().deserialize(None).unwrap();

        // add vertex label

        let mut label_index = None;
        let id_index = 0;

        for (i, s) in header.into_iter().enumerate() {
            let v = s.split(':').collect::<Vec<_>>();
            let prop_type = v[1];

            match prop_type {
                "LABEL" => {
                    assert!(label_index.replace(i).is_none());
                }
                _ if prop_type.starts_with("ID") => {
                    assert_eq!(id_index, i);
                }
                _ => (),
            }
        }

        let (tx, rx) = sync_channel::<Vec<String>>(1024);

        tokio::spawn(async move {
            for record in rdr.into_records() {
                let record: Vec<String> = record.unwrap().deserialize(None).unwrap();
                tx.send(record).unwrap();
            }
        });

        for record in rx {
            if let Some(label_index) = label_index {
                match record[label_index].as_str() {
                    "City" | "Country" | "Continent" => assert!(self
                        .place
                        .insert(
                            record[id_index].parse::<u64>().unwrap(),
                            record[label_index].clone()
                        )
                        .is_none()),
                    "University" | "Company" => assert!(self
                        .organisation
                        .insert(
                            record[id_index].parse::<u64>().unwrap(),
                            record[label_index].clone()
                        )
                        .is_none()),
                    _ => (),
                };
            }
            let label =
                label_index.map_or_else(|| label_name.clone(), |index| record[index].clone());

            *self.statistics.vertex_cardinality.entry(label).or_insert(0.0) += 1.0;
            *self.statistics.vertex_cardinality.entry("".to_owned()).or_insert(0.0) += 1.0;
        }
    }

    async fn import_edge(
        &mut self,
        path: std::path::PathBuf,
        (src_label, edge_label, dst_label): (String, String, String),
    ) {
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .delimiter(b'|')
            .from_path(path)
            .unwrap();

        let header: Vec<String> = rdr.headers().unwrap().deserialize(None).unwrap();

        let (src_id_index, dst_id_index) = (0, 1);

        let (mut src_label_map, mut dst_label_map) = (None, None);

        for (i, s) in header.into_iter().enumerate() {
            let v = s.split(':').collect::<Vec<_>>();
            let prop_type = v[1];

            match prop_type {
                _ if prop_type.starts_with("START_ID") => {
                    assert_eq!(src_id_index, i);
                    if src_label == "Organisation" {
                        assert!(src_label_map.replace(&self.organisation).is_none());
                    } else if src_label == "Place" {
                        assert!(src_label_map.replace(&self.place).is_none());
                    }
                }
                _ if prop_type.starts_with("END_ID") => {
                    assert_eq!(dst_id_index, i);
                    if dst_label == "Organisation" {
                        assert!(dst_label_map.replace(&self.organisation).is_none());
                    } else if dst_label == "Place" {
                        assert!(dst_label_map.replace(&self.place).is_none());
                    }
                }
                _ => (),
            }
        }

        let (tx, rx) = sync_channel::<Vec<String>>(1024);

        tokio::spawn(async move {
            for record in rdr.into_records() {
                let record: Vec<String> = record.unwrap().deserialize(None).unwrap();
                tx.send(record).unwrap();
            }
        });

        for record in rx {
            let src_id = record[src_id_index].parse::<u64>().unwrap();
            let dst_id = record[dst_id_index].parse::<u64>().unwrap();
            let src_label = src_label_map
                .map_or_else(|| src_label.clone(), |m| m.get(&src_id).unwrap().clone());
            let dst_label = dst_label_map
                .map_or_else(|| dst_label.clone(), |m| m.get(&dst_id).unwrap().clone());

            for src_key in [src_label, "".to_owned()] {
                let src_entry = self
                    .statistics.edge_cardinality
                    .entry(src_key)
                    .or_insert_with(HashMap::new);
                for edge_key in [edge_label.clone(), "".to_owned()] {
                    let edge_entry = src_entry.entry(edge_key).or_insert_with(HashMap::new);
                    for dst_key in [dst_label.clone(), "".to_owned()] {
                        let dst_entry = edge_entry.entry(dst_key).or_insert(0.0);
                        *dst_entry += 1.0;
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
enum LabelName {
    Vertex(String),
    Edge(String, String, String),
}

fn resolve_file_name(path: &std::path::Path) -> LabelName {
    let (src_name, edge_name, dst_name) = {
        let v = path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .split('_')
            .collect::<Vec<_>>();
        (v[0], v[1], v[2])
    };

    let resolve_edge_name = |name| {
        Some(match name {
            "isPartOf" => "IS_PART_OF",
            "isSubclassOf" => "IS_SUBCLASS_OF",
            "isLocatedIn" => "IS_LOCATED_IN",
            "hasType" => "HAS_TYPE",
            "hasCreator" => "HAS_CREATOR",
            "replyOf" => "REPLY_OF",
            "containerOf" => "CONTAINER_OF",
            "hasMember" => "HAS_MEMBER",
            "hasModerator" => "HAS_MODERATOR",
            "hasTag" => "HAS_TAG",
            "hasInterest" => "HAS_INTEREST",
            "knows" => "KNOWS",
            "likes" => "LIKES",
            "studyAt" => "STUDY_AT",
            "workAt" => "WORK_AT",
            _ => return None,
        })
    };

    let resolve_vertex_name = |name| {
        Some(match name {
            "place" => "Place",
            "organisation" => "Organisation",
            "tagclass" => "TagClass",
            "tag" => "Tag",
            "comment" => "Comment",
            "forum" => "Forum",
            "person" => "Person",
            "post" => "Post",
            _ => return None,
        })
    };

    match (
        resolve_vertex_name(src_name),
        resolve_edge_name(edge_name),
        resolve_vertex_name(dst_name),
    ) {
        (Some(src_name), Some(edge_name), Some(dst_name)) => LabelName::Edge(
            src_name.to_string(),
            edge_name.to_string(),
            dst_name.to_string(),
        ),
        (Some(vertex_name), None, None) => LabelName::Vertex(vertex_name.to_string()),
        _ => panic!(
            "illegal label name {:?} {:?} {:?}",
            src_name, edge_name, dst_name
        ),
    }
}

#[tokio::main]
async fn main() {
    let config = Config::parse();

    let mut paths = std::fs::read_dir(std::path::Path::new(&config.csv_dir).join("static"))
        .unwrap()
        .chain(std::fs::read_dir(std::path::Path::new(&config.csv_dir).join("dynamic")).unwrap())
        .map(|path| path.unwrap().path())
        .collect::<Vec<_>>();

    // order vertex files before edge files
    paths.sort_by_cached_key(|path| {
        path.file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .split('_')
            .count()
    });

    let mut context = Context::default();

    for path in paths {
        println!("import {:?}", path.as_os_str());
        let label_name = resolve_file_name(&path);
        match label_name {
            LabelName::Vertex(label) => context.import_vertex(path, label).await,
            LabelName::Edge(src_label, edge_label, dst_label) => {
                context
                    .import_edge(path, (src_label, edge_label, dst_label))
                    .await
            }
        }
    }

    // println!("{}", serde_json::to_string_pretty(&context.statistics).unwrap());
    let file = std::fs::File::create(&config.output_file).unwrap();
    let mut writer = std::io::BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, &context.statistics).unwrap();
    writer.flush().unwrap();

    // let file = std::fs::File::open(&config.output_file).unwrap();
    // let mut reader = std::io::BufReader::new(file);
    // let statistics: Statistics = serde_json::from_reader(&mut reader).unwrap();
    // println!("{}", serde_json::to_string_pretty(&statistics).unwrap());
}
