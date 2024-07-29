use std::collections::HashMap;
use std::path::PathBuf;
use std::{io::BufRead, process::Command, thread, time::Duration};

use crossbeam::channel::Sender;
use regex::Regex;

use crate::app::AppMessage;
use crate::app::Job;

struct JobWatcher {
    app: Sender<AppMessage>,
    interval: Duration,
    squeue_args: Vec<String>,
    sacct_args: Vec<String>,
    job_cache: HashMap<String, Job>,
}

pub struct JobWatcherHandle {}

impl JobWatcher {
    fn new(
        app: Sender<AppMessage>,
        interval: Duration,
        squeue_args: Vec<String>,
        sacct_args: Vec<String>,
    ) -> Self {
        Self {
            app,
            interval,
            squeue_args,
            sacct_args,
            job_cache: HashMap::new(),
        }
    }

    fn get_running_jobs(&self) -> Vec<Job> {
        let output_separator = "###turm###";
        let fields = [
            "jobid",
            "name",
            "state",
            "username",
            "timeused",
            "tres-alloc",
            "partition",
            "nodelist",
            "stdout",
            "stderr",
            "command",
            "statecompact",
            "reason",
            "qos",
            "ArrayJobID",  // %A
            "ArrayTaskID", // %a
            "NodeList",    // %N
            "WorkDir",     // for fallback
        ];
        let output_format = fields
            .map(|s| s.to_owned() + ":" + output_separator)
            .join(",");
        Command::new("squeue")
            .args(&self.squeue_args)
            .arg("--array")
            .arg("--noheader")
            .arg("--Format")
            .arg(&output_format)
            .output()
            .expect("failed to execute process")
            .stdout
            .lines()
            .map(|l| l.unwrap().trim().to_string())
            .filter_map(|l| {
                let parts: Vec<_> = l.split(output_separator).collect();

                if parts.len() != fields.len() + 1 {
                    return None;
                }

                let id = parts[0];
                let name = parts[1];
                let state = parts[2];
                let user = parts[3];
                let time = parts[4];
                let tres = parts[5];
                let partition = parts[6];
                let nodelist = parts[7];
                let stdout = parts[8];
                let stderr = parts[9];
                let command = parts[10];
                let state_compact = parts[11];
                let reason = parts[12];
                let qos = parts[13];

                let array_job_id = parts[14];
                let array_task_id = parts[15];
                let node_list = parts[16];
                let working_dir = parts[17];

                Some(Job {
                    job_id: id.to_owned(),
                    array_id: array_job_id.to_owned(),
                    array_step: match array_task_id {
                        "N/A" => None,
                        _ => Some(array_task_id.to_owned()),
                    },
                    name: name.to_owned(),
                    state: state.to_owned(),
                    state_compact: state_compact.to_owned(),
                    reason: if reason == "None" {
                        None
                    } else {
                        Some(reason.to_owned())
                    },
                    qos: qos.to_owned(),
                    user: user.to_owned(),
                    time: time.to_owned(),
                    tres: tres.to_owned(),
                    partition: partition.to_owned(),
                    nodelist: nodelist.to_owned(),
                    command: command.to_owned(),
                    stdout: Self::resolve_path(
                        stdout,
                        array_job_id,
                        array_task_id,
                        id,
                        node_list,
                        user,
                        name,
                        working_dir,
                    ),
                    stderr: Self::resolve_path(
                        stderr,
                        array_job_id,
                        array_task_id,
                        id,
                        node_list,
                        user,
                        name,
                        working_dir,
                    ), // TODO fill all fields
                })
            })
            .collect()
    }

    fn get_finished_jobs(&self) -> Vec<Job> {
        let output_separator = "###turm###";
        // Not all fields we need to create a Job are available via `sacct`
        // (most notably, stdout/stderr are missing on our cluster). So we only grab
        // some from a cache. On the other hand, we still want as many fields as
        // possible so that these are useful even if turm just started and the
        // cache is empty.
        let fields = [
            "jobid",
            "jobname",
            "state",
            "user",
            "elapsed",
            "alloctres",
            "partition",
            "nodelist",
            "submitline",
            "reason",
            "qos",
        ];
        let output_format = fields.join(",");
        let mut command = Command::new("sacct");
        command
            .args(&self.sacct_args)
            .arg("--array")
            .arg("--noheader")
            .arg("--format")
            .arg(&output_format)
            .arg("--delimiter")
            .arg(output_separator)
            .arg("-X")
            .arg("--parsable")
            .arg("--starttime")
            .arg("now-1hours")
            .arg("--endtime")
            .arg("now")
            .arg("--state")
            .arg("COMPLETED,CANCELLED,FAILED,TIMEOUT,PREEMPTED,OUT_OF_MEMORY");

        let out = command.output().expect("failed to execute process").stdout;

        out.lines()
            .map(|l| l.unwrap().trim().to_string())
            .filter_map(|l| {
                let parts: Vec<_> = l.split(output_separator).collect();

                if parts.len() != fields.len() + 1 {
                    return None;
                }

                let id = parts[0];
                let name = parts[1];
                let state = parts[2];
                let user = parts[3];
                let time = parts[4];
                let tres = parts[5];
                let partition = parts[6];
                let nodelist = parts[7];
                let command = parts[8]
                    // Remove the `sbatch` part of the command and slurm arguments.
                    // That matches the `squeue` "command" field.
                    .split_whitespace()
                    .skip_while(|&arg| arg.starts_with("sbatch") || arg.starts_with('-'))
                    .collect::<Vec<_>>()
                    .join(" ");
                let command = if command.is_empty() {
                    parts[8].to_owned()
                } else {
                    command
                };
                let reason = parts[9];
                let qos = parts[10];

                let state_compact = match state {
                    "RUNNING" => "R",
                    "PENDING" => "PD",
                    "COMPLETED" => "CD",
                    "CANCELLED" => "CA",
                    "FAILED" => "F",
                    "TIMEOUT" => "TO",
                    "NODE_FAIL" => "NF",
                    "PREEMPTED" => "PR",
                    "SUSPENDED" => "S",
                    _ => state, // Use the full state if it's not one of the known ones
                };

                // It seems sacct doesn't expose array ids, so we get them manually
                let (array_job_id, array_task_id) = if id.contains('_') {
                    let parts: Vec<&str> = id.split('_').collect();
                    if parts.len() == 2 {
                        (parts[0], parts[1])
                    } else {
                        (id, "N/A")
                    }
                } else {
                    (id, "N/A")
                };

                Some(Job {
                    job_id: id.to_owned(),
                    array_id: array_job_id.to_owned(),
                    array_step: match array_task_id {
                        "N/A" => None,
                        _ => Some(array_task_id.to_owned()),
                    },
                    name: name.to_owned(),
                    state: state.to_owned(),
                    state_compact: state_compact.to_owned(),
                    reason: if reason == "None" {
                        None
                    } else {
                        Some(reason.to_owned())
                    },
                    qos: qos.to_owned(),
                    user: user.to_owned(),
                    time: time.to_owned(),
                    tres: tres.to_owned(),
                    partition: partition.to_owned(),
                    nodelist: nodelist.to_owned(),
                    command: command.to_owned(),
                    stdout: None,
                    stderr: None,
                })
            })
            .collect()
    }

    fn run(&mut self) -> Self {
        loop {
            let running_jobs = self.get_running_jobs();
            let finished_jobs = self.get_finished_jobs();

            // Update cache with running jobs
            for job in &running_jobs {
                self.job_cache.insert(job.job_id.clone(), job.clone());
            }

            // Fill in missing info for finished jobs
            let finished_jobs = finished_jobs
                .into_iter()
                .map(|mut job| {
                    if let Some(cached_job) = self.job_cache.get(&job.job_id) {
                        job.stdout = cached_job.stdout.clone();
                        job.stderr = cached_job.stderr.clone();
                    }
                    job
                })
                .collect::<Vec<Job>>();

            // Combine running and finished jobs
            let jobs: Vec<Job> = running_jobs
                .into_iter()
                .chain(finished_jobs.into_iter())
                .collect();

            // Clean up cache (remove jobs that are no longer running or finished)
            let active_job_ids: std::collections::HashSet<String> =
                jobs.iter().map(|job| job.job_id.clone()).collect();
            self.job_cache
                .retain(|job_id, _| active_job_ids.contains(job_id));

            self.app.send(AppMessage::Jobs(jobs)).unwrap();
            thread::sleep(self.interval);
        }
    }

    fn resolve_path(
        path: &str,
        array_master: &str,
        array_id: &str,
        id: &str,
        host: &str,
        user: &str,
        name: &str,
        working_dir: &str,
    ) -> Option<PathBuf> {
        // see https://slurm.schedmd.com/sbatch.html#SECTION_%3CB%3Efilename-pattern%3C/B%3E
        lazy_static::lazy_static! {
            static ref RE: Regex = Regex::new(r"%(%|A|a|J|j|N|n|s|t|u|x)").unwrap();
        }

        let mut path = path.to_owned();
        let slurm_no_val = "4294967294";
        let array_id = if array_id == "N/A" {
            slurm_no_val
        } else {
            array_id
        };

        if path.is_empty() {
            // never happens right now, because `squeue -O stdout` seems to always return something
            path = if array_id == slurm_no_val {
                PathBuf::from(working_dir).join("slurm-%J.out")
            } else {
                PathBuf::from(working_dir).join("slurm-%A_%a.out")
            }
            .to_str()
            .unwrap()
            .to_owned()
        };

        for cap in RE
            .captures_iter(&path.clone())
            .collect::<Vec<_>>() // TODO: this is stupid, there has to be a better way to reverse the captures...
            .iter()
            .rev()
        {
            let m = cap.get(0).unwrap();
            let replacement = match m.as_str() {
                "%%" => "%",
                "%A" => array_master,
                "%a" => array_id,
                "%J" => id,
                "%j" => id,
                "%N" => host.split(',').next().unwrap_or(host),
                "%n" => "0",
                "%s" => "batch",
                "%t" => "0",
                "%u" => user,
                "%x" => name,
                _ => unreachable!(),
            };

            path.replace_range(m.range(), replacement);
        }

        Some(PathBuf::from(path))
    }
}

impl JobWatcherHandle {
    pub fn new(
        app: Sender<AppMessage>,
        interval: Duration,
        squeue_args: Vec<String>,
        sacct_args: Vec<String>,
    ) -> Self {
        let mut actor = JobWatcher::new(app, interval, squeue_args, sacct_args);
        thread::spawn(move || actor.run());

        Self {}
    }
}
