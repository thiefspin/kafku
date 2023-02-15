use crate::config::KafkaConfig;
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

const DB_PATH: &str = "./data/db.json";

#[derive(Error, Debug)]
pub enum Error {
    #[error("error reading the DB file: {0}")]
    ReadDBError(#[from] io::Error),
    #[error("error parsing the DB file: {0}")]
    ParseDBError(#[from] serde_json::Error),
}

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
    Home,
    Users,
    Topics,
}

impl From<MenuItem> for usize {
    fn from(input: MenuItem) -> usize {
        match input {
            MenuItem::Home => 0,
            MenuItem::Users => 1,
            MenuItem::Topics => 2,
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let kafka_config = config::get(args[1].to_string()).unwrap();

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

    let menu_titles = vec!["Home", "Users", "Add", "Delete", "Topics", "Quit"];
    let mut active_menu_item = MenuItem::Home;
    let mut user_list_state = ListState::default();
    let mut topic_list_state = ListState::default();
    user_list_state.select(Some(0));
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
                        Constraint::Min(2),
                        Constraint::Length(3),
                    ]
                    .as_ref(),
                )
                .split(size);

            let copyright = Paragraph::new("User-CLI 2023 - all rights reserved")
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

            rect.render_widget(tabs, chunks[0]);
            match active_menu_item {
                MenuItem::Home => rect.render_widget(render_home(), chunks[1]),
                MenuItem::Topics => {
                    let topics_chunks = Layout::default()
                        .direction(Direction::Horizontal)
                        .constraints(
                            [Constraint::Percentage(20), Constraint::Percentage(80)].as_ref(),
                        )
                        .split(chunks[1]);
                    let (left, right) = render_topics(&topic_list_state, topic_list.clone());
                    rect.render_stateful_widget(left, topics_chunks[0], &mut topic_list_state);
                    rect.render_widget(right, topics_chunks[1]);
                }
                MenuItem::Users => {
                    let users_chunks = Layout::default()
                        .direction(Direction::Horizontal)
                        .constraints(
                            [Constraint::Percentage(20), Constraint::Percentage(80)].as_ref(),
                        )
                        .split(chunks[1]);
                    let (left, right) = render_users(&user_list_state);
                    rect.render_stateful_widget(left, users_chunks[0], &mut user_list_state);
                    rect.render_widget(right, users_chunks[1]);
                }
            }
            rect.render_widget(copyright, chunks[2]);
        })?;

        match rx.recv()? {
            Event::Input(event) => match event.code {
                KeyCode::Char('q') => {
                    disable_raw_mode()?;
                    terminal.show_cursor()?;
                    break;
                }
                KeyCode::Char('h') => active_menu_item = MenuItem::Home,
                KeyCode::Char('u') => active_menu_item = MenuItem::Users,
                KeyCode::Char('t') => active_menu_item = MenuItem::Topics,
                KeyCode::Char('a') => {
                    add_random_user_to_db().expect("can add new random user");
                }
                KeyCode::Char('d') => {
                    remove_user_at_index(&mut user_list_state).expect("can remove user");
                }
                KeyCode::Down => match active_menu_item {
                    MenuItem::Users => {
                        if let Some(selected) = user_list_state.selected() {
                            let amount_users = read_db().expect("can fetch user list").len();
                            if selected >= amount_users - 1 {
                                user_list_state.select(Some(0));
                            } else {
                                user_list_state.select(Some(selected + 1));
                            }
                        }
                    }
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
                    MenuItem::Users => {
                        if let Some(selected) = user_list_state.selected() {
                            let amount_users = read_db().expect("can fetch user list").len();
                            if selected > 0 {
                                user_list_state.select(Some(selected - 1));
                            } else {
                                user_list_state.select(Some(amount_users - 1));
                            }
                        }
                    }
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

fn render_topics<'a>(
    topic_list_state: &ListState,
    topic_list: Vec<TopicData>,
) -> (List<'a>, Table<'a>) {
    let topics = Block::default()
        .borders(Borders::ALL)
        .style(Style::default().fg(Color::White))
        .title("Topics")
        .border_type(BorderType::Plain);

    // let user_list = read_db().expect("can fetch user list");
    let items: Vec<_> = topic_list
        .iter()
        .map(|topic| {
            ListItem::new(Spans::from(vec![Span::styled(
                topic.name.clone(),
                Style::default(),
            )]))
        })
        .collect();

    let selected_topic = topic_list
        .get(
            topic_list_state
                .selected()
                .expect("there is always a selected topic"),
        )
        .expect("exists")
        .clone();

    let list = List::new(items).block(topics).highlight_style(
        Style::default()
            .bg(Color::Yellow)
            .fg(Color::Black)
            .add_modifier(Modifier::BOLD),
    );

    let rows: Vec<Row> = selected_topic.partitions.iter().map(|p| {
        Row::new(vec![
            Cell::from(Span::raw(p.id.to_string())),
            Cell::from(Span::raw(p.leader.to_string())),
            Cell::from(Span::raw(p.available.to_string())),
            Cell::from(Span::raw(p.offset.to_string()))
        ])
    }).collect();

    let topic_detail = Table::new(rows).header(Row::new(vec![
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

    // let topic_detail = Table::new(vec![Row::new(vec![
    //     Cell::from(Span::raw(selected_topic.)),
    //     Cell::from(Span::raw("Topic detail 2")),
    //     Cell::from(Span::raw("Topic detail 2")),
    //     Cell::from(Span::raw("Topic detail 2")),
    //     Cell::from(Span::raw("Topic detail 2")),
    // ])])
    // let topic_detail = Table::new(vec![Row::new(vec![
    //     Cell::from(Span::raw(selected_topic.id.to_string())),
    //     Cell::from(Span::raw(selected_topic.name)),
    //     Cell::from(Span::raw(selected_topic.role)),
    //     Cell::from(Span::raw(selected_topic.age.to_string())),
    //     Cell::from(Span::raw(selected_topic.created_at.to_string())),
    // ])])
    // .header(Row::new(vec![
    //     Cell::from(Span::styled(
    //         "Name",
    //         Style::default().add_modifier(Modifier::BOLD),
    //     )),
    //     Cell::from(Span::styled(
    //         "Name",
    //         Style::default().add_modifier(Modifier::BOLD),
    //     )),
    //     Cell::from(Span::styled(
    //         "Category",
    //         Style::default().add_modifier(Modifier::BOLD),
    //     )),
    //     Cell::from(Span::styled(
    //         "Age",
    //         Style::default().add_modifier(Modifier::BOLD),
    //     )),
    //     Cell::from(Span::styled(
    //         "Created At",
    //         Style::default().add_modifier(Modifier::BOLD),
    //     )),
    // ]))
    // .block(
    //     Block::default()
    //         .borders(Borders::ALL)
    //         .style(Style::default().fg(Color::White))
    //         .title("Detail")
    //         .border_type(BorderType::Plain),
    // )
    // .widths(&[
    //     Constraint::Percentage(20),
    //     Constraint::Percentage(20),
    //     Constraint::Percentage(20),
    //     Constraint::Percentage(5),
    //     Constraint::Percentage(5),
    // ]);

    // (list, topic_detail)
}

fn render_home<'a>() -> Paragraph<'a> {
    let home = Paragraph::new(vec![
        Spans::from(vec![Span::raw("")]),
        Spans::from(vec![Span::raw("Welcome")]),
        Spans::from(vec![Span::raw("")]),
        Spans::from(vec![Span::raw("to")]),
        Spans::from(vec![Span::raw("")]),
        Spans::from(vec![Span::styled(
            "user-CLI",
            Style::default().fg(Color::LightBlue),
        )]),
        Spans::from(vec![Span::raw("")]),
        Spans::from(vec![Span::raw("Press 'u' to access users, 'a' to add random new users and 'd' to delete the currently selected user.")]),
    ])
    .alignment(Alignment::Center)
    .block(
        Block::default()
            .borders(Borders::ALL)
            .style(Style::default().fg(Color::White))
            .title("Home")
            .border_type(BorderType::Plain),
    );
    home
}

fn render_users<'a>(user_list_state: &ListState) -> (List<'a>, Table<'a>) {
    let users = Block::default()
        .borders(Borders::ALL)
        .style(Style::default().fg(Color::White))
        .title("Users")
        .border_type(BorderType::Plain);

    let user_list = read_db().expect("can fetch user list");
    let items: Vec<_> = user_list
        .iter()
        .map(|user| {
            ListItem::new(Spans::from(vec![Span::styled(
                user.name.clone(),
                Style::default(),
            )]))
        })
        .collect();

    let selected_user = user_list
        .get(
            user_list_state
                .selected()
                .expect("there is always a selected user"),
        )
        .expect("exists")
        .clone();

    let list = List::new(items).block(users).highlight_style(
        Style::default()
            .bg(Color::Yellow)
            .fg(Color::Black)
            .add_modifier(Modifier::BOLD),
    );

    let user_detail = Table::new(vec![Row::new(vec![
        Cell::from(Span::raw(selected_user.id.to_string())),
        Cell::from(Span::raw(selected_user.name)),
        Cell::from(Span::raw(selected_user.role)),
        Cell::from(Span::raw(selected_user.age.to_string())),
        Cell::from(Span::raw(selected_user.created_at.to_string())),
    ])])
    .header(Row::new(vec![
        Cell::from(Span::styled(
            "ID",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Cell::from(Span::styled(
            "Name",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Cell::from(Span::styled(
            "Category",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Cell::from(Span::styled(
            "Age",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Cell::from(Span::styled(
            "Created At",
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
        Constraint::Percentage(5),
        Constraint::Percentage(20),
        Constraint::Percentage(20),
        Constraint::Percentage(5),
        Constraint::Percentage(20),
    ]);

    (list, user_detail)
}

fn read_db() -> Result<Vec<User>, Error> {
    let db_content = fs::read_to_string(DB_PATH)?;
    let parsed: Vec<User> = serde_json::from_str(&db_content)?;
    Ok(parsed)
}

fn add_random_user_to_db() -> Result<Vec<User>, Error> {
    let mut rng = rand::thread_rng();
    let db_content = fs::read_to_string(DB_PATH)?;
    let mut parsed: Vec<User> = serde_json::from_str(&db_content)?;
    let admin_user = match rng.gen_range(0, 1) {
        0 => "Admin",
        _ => "User",
    };

    let random_user = User {
        id: rng.gen_range(0, 9999999),
        name: rng.sample_iter(Alphanumeric).take(10).collect(),
        role: admin_user.to_owned(),
        age: rng.gen_range(1, 15),
        created_at: Utc::now(),
    };

    parsed.push(random_user);
    fs::write(DB_PATH, &serde_json::to_vec(&parsed)?)?;
    Ok(parsed)
}

fn remove_user_at_index(user_list_state: &mut ListState) -> Result<(), Error> {
    if let Some(selected) = user_list_state.selected() {
        let db_content = fs::read_to_string(DB_PATH)?;
        let mut parsed: Vec<User> = serde_json::from_str(&db_content)?;
        parsed.remove(selected);
        fs::write(DB_PATH, &serde_json::to_vec(&parsed)?)?;
        if selected > 0 {
            user_list_state.select(Some(selected - 1));
        } else {
            user_list_state.select(Some(0));
        }
    }
    Ok(())
}
