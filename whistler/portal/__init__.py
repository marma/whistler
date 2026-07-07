"""Whistler portal: web front door for desktop sessions.

A thin aiohttp app that lets a browser launch and connect to DesktopSessions.
The display backend is the Selkies 2.x (pixelflux) "websockets" viewer — the
in-pod server streams H.264 over plain WebSockets and the portal reverse-proxies
it to the browser (no guacd, no coturn/TURN). It also serves a web terminal
(xterm.js over ``kubectl exec``). State stays in CRs — the portal holds no
session state of its own.
"""
