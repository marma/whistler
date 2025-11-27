# Whistler

Whistler is a Kubernetes Operator that provisions on-demand and persistant instances in a cluster through SSH.

**Note**: Whistler is currently in alpha and is not ready for production use.
**Note**: Whistler is also a test of using Google Antigravity, that experience it detailed [here](antigravity.md)

Whistler has the following features:

- A simple administrative TUI used to manage existing sessions and create templates for future sessions
- Ease of use: users use standard SSH to connect to existing pod or create one on-demand
- It allows for configuration of templates used to start interactive sessions through an administrative TUI available to users
- Sessions can be preemtible, ephemeral, or persistent

**Note**: preemtible and ephemeral sessions are not yet supported.

## Install

### Installation through Helm Chart

```bash
helm repo add whistler https://github.com/marma/whistler
helm install whistler whistler/whistler
```

Example `values.yaml`:

```yaml
whistler:
  users:
    - name: someuser
      publicKeys:
        - ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC6...
```

# Design

Whistler is implemented as a Kubernetes Operator that uses the Gateway API and AsyncSSH to expose SSH sessions and an administrative interface to users. Interactive sessions are either connected to using the interface, or through simply ssh:ing to the service which will then create a pod on-the-fly. 

Whistler will on install also create number of CRDs to keep track of instances and templates. There is, by design, no database used to store data, rather state is stored in the cluster as CRs.

It uses the following CRDs:

- `WhistlerTemplate`: A template for creating instances
- `WhistlerInstance`: An instance of a template

One PV, through a PVC, is created for each user to store their home directory, unless specified otherwise in the template or instance. Other volumes can be mounted as needed.

# Usage

Example:

Connect to the administrative interface for user `someuser`:

```
ssh someuser@whistler.example.com
```

Create and connect to an ephemeral session using template `small`:

```
ssh someuser-small@whistler.example.com
```

Connect to an existing instance with the name `123`: 

```
ssh someuser-123@whistler.example.com
```

# Implementation

Software used:

- [asyncssh](https://github.com/asyncssh/asyncssh)
- [Textual](https://github.com/Textualize/textual) 
- [KOPF](https://kopf.readthedocs.io/en/stable/)
