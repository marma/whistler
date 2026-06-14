"""Whistler portal: web front door for desktop sessions.

A thin aiohttp app that lets a browser launch and connect to DesktopSessions.
It bridges the browser's guacamole-common-js client to a shared guacd over a
WebSocket, performing the guacd handshake server-side (like guacamole-lite) and
then relaying the Guacamole protocol stream. State stays in CRs — the portal
holds no session state of its own.
"""
