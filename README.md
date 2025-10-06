# podcast
Open source utilities for The Pretrained Pod

## Mirror

Utility to invert our Riverside recording window, so we can broadcast the flipped secondary window to our teleprompter (so I can look at the camera while I'm talking to Richard). Our Makefile takes care of building a production-ready .app versus the Swift CLI utilities that only output an executable.

```bash
tccutil reset ScreenCapture com.pretrainedpod.mirror
make run
```
