# SUPERSEDED — 并发 k 发已否决

此稿（同一 offset 同时打多条路 / 首窗 k=3）**审核不通过**。

治标不治本：第一次并发了，后面要么继续浪费带宽，要么又回到 sticky；流进度仍绑在「要不要再发一份」而不是「不要绑死在一条 TCP 上」。

现行方案：**一包只先发一次，按发出路径 RTT 的倍数换路重传。**

见 [`design-path-agnostic-offset.md`](design-path-agnostic-offset.md)。
