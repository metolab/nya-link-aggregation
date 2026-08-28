# 发布

版本号在根目录 [`Cargo.toml`](../Cargo.toml) 的 `[workspace.package] version`。GitHub Release 由 tag 触发，不手搓附件。

## 产物

[Build](https://github.com/metolab/nya-link-aggregation/actions/workflows/build.yml) 只打两个目标：

| 归档 | 平台 |
| --- | --- |
| `nya-x86_64-unknown-linux-gnu.tar.gz` | Linux x86_64 |
| `nya-aarch64-apple-darwin.tar.gz` | macOS Apple Silicon |

每个包里是 `nya-client`、`nya-server`、`README.md`、`LICENSE`。Release 另外带一份 `SHA256SUMS`。

main 上每次推送也会构建，包挂在该次 run 的 Artifacts，不建 Release。

## 打一次版本

1. 把 `Cargo.toml` 里的 `version` 改成要发的号（例如 `0.1.1`）。lockfile 不用动，workspace 成员跟 workspace version。
2. 提交，例如 `Release v0.1.1`。
3. 确认 main 上的 CI 是绿的。
4. 打 **annotated** tag，名字必须是 `v` + 版本号，和 Cargo 一致：

```bash
git tag -a v0.1.1 -m "v0.1.1"
git push origin main --tags
```

5. 推 tag 会跑 Build：两个平台编完后，`release` job 用 `softprops/action-gh-release` 建 [GitHub Release](https://github.com/metolab/nya-link-aggregation/releases)，标题是 tag 名，附件是上面两个 tar.gz + `SHA256SUMS`，notes 用 GitHub 自动生成的 changelog。
6. 打开 Release 页看附件是否齐全。需要改说明就在网页上改，不必重打 tag。

也可以在 Actions 里对 Build 点 **Run workflow**（`workflow_dispatch`），只出 Artifacts，不发 Release。

## 规则

- 只给 `v*` tag 发 Release。别的 tag、main、PR 都不会发。
- 不要改已发布 tag 的提交再 `tag -f`；错了就升一个补丁号。
- 二进制是 `--release --locked`，对应仓库里的 `Cargo.lock`。
