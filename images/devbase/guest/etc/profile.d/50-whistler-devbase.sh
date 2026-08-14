# PATH bits for the Whistler devbase image (see images/devbase/README.md).
# Sourced by login shells; `ssh <host> <command>` (no login shell) does not get
# it, so anything that must work non-interactively should use absolute paths.
#
# pixi itself is /usr/local/bin/pixi (already on PATH). This adds what pixi
# *installs*: `pixi global install` puts shims in $HOME/.pixi/bin, and $HOME is
# the user's PVC over NFS — so a user's global tools survive the session, the
# image rebuild, and the move between devbase variants. PIXI_HOME is left at
# its default for the same reason.
case ":${PATH}:" in
  *":$HOME/.pixi/bin:"*) ;;
  *) [ -n "${HOME:-}" ] && PATH="$HOME/.pixi/bin:$PATH" ;;
esac

# CUDA SDK (the -cuda-dev variant only; the dir does not exist elsewhere).
# CUDA_HOME is the point of this block — the SDK's *binaries* are symlinked into
# /usr/local/bin at bake time precisely because profile.d is not enough for
# `ssh <session>.w nvcc ...`. Keeping the bin dir on PATH too is harmless and
# covers anything the symlink loop skipped.
if [ -d /usr/local/cuda/bin ]; then
  case ":${PATH}:" in
    *":/usr/local/cuda/bin:"*) ;;
    *) PATH="$PATH:/usr/local/cuda/bin" ;;
  esac
  export CUDA_HOME=/usr/local/cuda
fi

export PATH
