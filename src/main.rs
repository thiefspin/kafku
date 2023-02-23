use crate::{config::KafkaConfig, kafka_client::parse_message};
use crate::kafka_client::SimpleKafkaClient;
use chrono::prelude::*;
use crossterm::{
    event::{self, Event as CEvent, KeyCode},
    terminal::{disable_raw_mode, enable_raw_mode},
};
use kafka_client::TopicData;
use rand::{distributions::Alphanumeric, prelude::*};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use std::{env, str};
use thiserror::Error;
use tui::{
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Span, Spans},
    widgets::{
        Block, BorderType, Borders, Cell, List, ListItem, ListState, Paragraph, Row, Table, Tabs,
    },
    Terminal,
};

mod config;
mod kafka_client;

enum Event<I> {
    Input(I),
    Tick,
}

#[derive(Serialize, Deserialize, Clone)]
struct User {
    id: usize,
    name: String,
    role: String,
    age: usize,
    created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug)]
enum MenuItem {
    Topics,
}

impl From<MenuItem> for usize {
    fn from(input: MenuItem) -> usize {
        match input {
            MenuItem::Topics => 0,
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let kafka_config: KafkaConfig = config::get(args[1].to_string()).unwrap();

    println!("Using host: {}", kafka_config.broker());

    let kafka_hosts: Vec<String> = vec![kafka_config.broker().to_string()];
    let client = SimpleKafkaClient {
        hosts: kafka_hosts.clone(),
    };

    let topic_list = client.list_topic_details();

    enable_raw_mode().expect("can run in raw mode");

    let (tx, rx) = mpsc::channel();
    let tick_rate = Duration::from_millis(200);
    thread::spawn(move || {
        let mut last_tick = Instant::now();
        loop {
            let timeout = tick_rate
                .checked_sub(last_tick.elapsed())
                .unwrap_or_else(|| Duration::from_secs(0));

            if event::poll(timeout).expect("poll works") {
                if let CEvent::Key(key) = event::read().expect("can read events") {
                    tx.send(Event::Input(key)).expect("can send events");
                }
            }

            if last_tick.elapsed() >= tick_rate {
                if let Ok(_) = tx.send(Event::Tick) {
                    last_tick = Instant::now();
                }
            }
        }
    });

    let stdout = io::stdout();
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    let menu_titles = vec!["Topics", "Pull", "Clear", "Quit"];
    let mut active_menu_item = MenuItem::Topics;
    let mut topic_list_state = ListState::default();
    let mut msgs: Vec<String> = vec![];
    topic_list_state.select(Some(0));

    loop {
        terminal.draw(|rect| {
            let size = rect.size();
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(2)
                .constraints(
                    [
                        Constraint::Length(3),
                        Constraint::Length(3),
                        Constraint::Min(2),
                        Constraint::Length(3),
                    ]
                    .as_ref(),
                )
                .split(size);

            let copyright = Paragraph::new("Kafka-CLI 2023 - all rights reserved")
                .style(Style::default().fg(Color::LightCyan))
                .alignment(Alignment::Center)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .style(Style::default().fg(Color::White))
                        .title("Copyright")
                        .border_type(BorderType::Plain),
                );

            let menu = menu_titles
                .iter()
                .map(|t| {
                    let (first, rest) = t.split_at(1);
                    Spans::from(vec![
                        Span::styled(
                            first,
                            Style::default()
                                .fg(Color::Yellow)
                                .add_modifier(Modifier::UNDERLINED),
                        ),
                        Span::styled(rest, Style::default().fg(Color::White)),
                    ])
                })
                .collect();

            let tabs = Tabs::new(menu)
                .select(active_menu_item.into())
                .block(Block::default().title("Menu").borders(Borders::ALL))
                .style(Style::default().fg(Color::White))
                .highlight_style(Style::default().fg(Color::Yellow))
                .divider(Span::raw("|"));

            let hosts = Spans::from(vec![Span::styled(
                broker_info_label(client.list_brokers()),
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::UNDERLINED),
            )]);

            let topic_num = Spans::from(vec![Span::styled(
                num_topics_label(client.list_topics().len()),
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::UNDERLINED),
            )]);

            let info_tab = Tabs::new(vec![hosts, topic_num])
                .block(Block::default().title("Info").borders(Borders::ALL))
                .style(Style::default().fg(Color::White))
                .highlight_style(Style::default().fg(Color::Yellow))
                .divider(Span::raw("|"));

            rect.render_widget(tabs, chunks[0]);
            rect.render_widget(info_tab, chunks[1]);
            match active_menu_item {
                MenuItem::Topics => {
                    let topics_chunks = Layout::default()
                        .direction(Direction::Horizontal)
                        .constraints(
                            [
                                Constraint::Percentage(20),
                                Constraint::Percentage(40),
                                Constraint::Percentage(40),
                            ]
                            .as_ref(),
                        )
                        .split(chunks[2]);
                    let (left, right) = render_topics(&topic_list_state, topic_list.clone());
                    let messages = messages_block(msgs.clone());
                    rect.render_stateful_widget(left, topics_chunks[0], &mut topic_list_state);
                    rect.render_widget(right, topics_chunks[1]);
                    rect.render_widget(messages, topics_chunks[2]);
                }
            }
            rect.render_widget(copyright, chunks[3]);
        })?;

        match rx.recv()? {
            Event::Input(event) => match event.code {
                KeyCode::Char('q') => {
                    disable_raw_mode()?;
                    terminal.show_cursor()?;
                    break;
                }
                KeyCode::Char('t') => active_menu_item = MenuItem::Topics,
                KeyCode::Char('c') =>  {
                    msgs.clear();
                },
                KeyCode::Char('p') => {
                    let selected = get_selected_topic(&topic_list_state.clone(), topic_list.clone()).name;
                    let mut consumer = client.create_consumer(&selected);
                    for ms in consumer.poll().unwrap().iter() {
                        for m in ms.messages() {
                            let message = parse_message(m.value);
                            msgs.push(message)
                        }
                        consumer.consume_messageset(ms).unwrap();
                    }
                    consumer.commit_consumed().unwrap();
                }
                KeyCode::Down => match active_menu_item {
                    MenuItem::Topics => {
                        if let Some(selected) = topic_list_state.selected() {
                            let amount_topics = topic_list.len();
                            if selected >= amount_topics - 1 {
                                topic_list_state.select(Some(0));
                            } else {
                                topic_list_state.select(Some(selected + 1));
                            }
                        }
                    }
                    _ => {}
                },
                KeyCode::Up => match active_menu_item {
                    MenuItem::Topics => {
                        if let Some(selected) = topic_list_state.selected() {
                            let amount_topics = topic_list.len();
                            if selected > 0 {
                                topic_list_state.select(Some(selected - 1));
                            } else {
                                topic_list_state.select(Some(amount_topics - 1));
                            }
                        }
                    }
                    _ => {}
                },
                _ => {}
            },
            Event::Tick => {}
        }
    }

    Ok(())
}

fn messages_block<'a>(msgs: Vec<String>) -> List<'a> {
    let heading = Block::default()
        .borders(Borders::ALL)
        .style(Style::default().fg(Color::White))
        .title("Messages")
        .border_type(BorderType::Plain);

    let items: Vec<_> = msgs
        .iter()
        .map(|msg| {
            ListItem::new(Spans::from(vec![Span::styled(
                msg.clone(),
                Style::default(),
            )]))
        })
        .collect();
    return List::new(items).block(heading).highlight_style(
        Style::default()
            .bg(Color::Yellow)
            .fg(Color::Black)
            .add_modifier(Modifier::BOLD),
    );
}

fn broker_info_label(brokers: Vec<String>) -> String {
    return format!("{} {}", "Brokers:", brokers.join(", "));
}

fn num_topics_label(num: usize) -> String {
    return format!("{} {}", "Number of Topics:", num.to_string());
}

fn get_selected_topic(topic_list_state: &ListState, topic_list: Vec<TopicData>) -> TopicData {
    return topic_list
        .get(
            topic_list_state
                .selected()
                .expect("there is always a selected topic"),
        )
        .expect("exists")
        .clone();
}

fn render_topics<'a>(
    topic_list_state: &ListState,
    topic_list: Vec<TopicData>,
) -> (List<'a>, Table<'a>) {
    let topics = Block::default()
        .borders(Borders::ALL)
        .style(Style::default().fg(Color::White))
        .title("Topics")
        .border_type(BorderType::Plain);

    let items: Vec<_> = topic_list
        .iter()
        .map(|topic| {
            ListItem::new(Spans::from(vec![Span::styled(
                topic.name.clone(),
                Style::default(),
            )]))
        })
        .collect();

    let selected_topic = get_selected_topic(topic_list_state, topic_list);

    let list = List::new(items).block(topics).highlight_style(
        Style::default()
            .bg(Color::Yellow)
            .fg(Color::Black)
            .add_modifier(Modifier::BOLD),
    );

    let rows: Vec<Row> = selected_topic
        .partitions
        .iter()
        .map(|p| {
            Row::new(vec![
                Cell::from(Span::raw(p.id.to_string())),
                Cell::from(Span::raw(p.leader.to_string())),
                Cell::from(Span::raw(p.available.to_string())),
                Cell::from(Span::raw(p.offset.to_string())),
            ])
        })
        .collect();

    let topic_detail = Table::new(rows)
        .header(Row::new(vec![
            Cell::from(Span::styled(
                "Partition Id",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Cell::from(Span::styled(
                "Leader",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Cell::from(Span::styled(
                "Available",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Cell::from(Span::styled(
                "Offset",
                Style::default().add_modifier(Modifier::BOLD),
            )),
        ]))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .style(Style::default().fg(Color::White))
                .title("Detail")
                .border_type(BorderType::Plain),
        )
        .widths(&[
            Constraint::Percentage(20),
            Constraint::Percentage(20),
            Constraint::Percentage(20),
            Constraint::Percentage(5),
            Constraint::Percentage(5),
        ]);

    (list, topic_detail)
}
