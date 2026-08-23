# Droid

Droid is Factory's coding agent. T3 Code connects to the Factory Droid CLI on the machine running
the server, so you can use your own Factory subscription while working from the web, desktop, or
mobile app.

Droid support is in Early Access. Enable it from the Droid provider card in Settings after
installing and authenticating the CLI.

## Install And Log In

Install Factory Droid:

```bash
npm install -g @factory/cli
```

Then start Droid in a terminal:

```bash
droid
```

Follow the browser sign-in flow. Run this on the machine that runs the T3 Code server.

For API-key authentication, set `FACTORY_API_KEY` in the Droid provider's Environment variables
section in Settings. Mark it as sensitive so T3 Code stores it as a server secret and does not send
it back to the app after saving.

## Models And Reasoning

T3 Code fetches the available models from Droid dynamically. Each model advertises the reasoning
efforts it supports, and those choices appear with the model in the picker. The list can change as
Factory adds or updates models without requiring a T3 Code update.

## Permission Modes

T3 Code maps its permission modes onto Droid's command confirmation levels:

| T3 Code mode                   | Droid behavior                                    |
| ------------------------------ | ------------------------------------------------- |
| Supervised (approval required) | Confirms every command and file change            |
| Auto-accept edits              | Automatically allows edits and read-only commands |
| Auto                           | Also allows reversible commands without prompting |
| Full access                    | Allows all commands without prompting             |

Approvals appear inline in the conversation. Rejecting one returns control to Droid so it can adapt
or ask what to do next.

## Plan Mode

T3 Code's plan mode uses Droid's Spec Mode. Droid researches and writes a plan before implementation,
then presents the plan approval as an approval request in the conversation. Approve it to begin
implementation or reject it to keep refining the plan. On approval, Droid hands the work to an
implementation session in the same thread; the turn keeps streaming and the thread resumes onto the
implementation conversation afterwards.

## Context And Subagents

Droid compacts long conversations automatically, so the context meter shows the live context after
compaction rather than lifetime usage. When Droid delegates work to a subagent, it appears as a task
in the conversation with its own completion state.

## Session Resume

Droid sessions resume across T3 Code server restarts. Reopen the same thread and continue where you
left off instead of starting a new Droid conversation.

## Early Access

Droid support is still evolving. Model metadata, reasoning choices, approval behavior, and session
resume may change as the Factory CLI develops. If a session behaves unexpectedly, update Factory
Droid, refresh its status in Settings, and start a new thread if the existing session cannot resume.
