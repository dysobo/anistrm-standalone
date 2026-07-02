# ANiStrm Standalone

一个独立的 Open ANi STRM 生成器，带 Web 管理页面和定时增量更新能力。它会把番剧视频生成 `.strm`，把字幕、NFO、图片等附属文件下载为原文件，方便 Emby/Jellyfin/Plex 等媒体库直接扫描。

## 功能

- 独立 FastAPI Web 页面，不依赖 MoviePilot。
- 支持选择季度，包括 `latest` 和 Open ANi 根目录下的特殊 `ANi` 文件夹。
- 支持手动运行、首次全量生成、定时增量更新。
- 支持 cron 配置、代理配置、运行日志查看。
- 视频文件生成 `.strm`：`.mp4`、`.mkv`、`.avi`、`.mov`、`.flv`、`.webm`。
- 附属文件直接下载：`.nfo`、`.srt`、`.vtt`、`.ass`、`.ssa`、`.smi`、`.jpg`、`.jpeg`、`.png`、`.webp`、`.zip`。
- 输出结构固定为：

```text
/strm/<番名>/<文件名>.strm
/strm/<番名>/<附属文件名>
```

## 快速开始

```bash
cp .env.example .env
docker compose up -d --build
```

默认访问地址：

```text
http://localhost:8080/
```

默认输出目录为当前项目下的 `./strm`，配置文件保存在 `./data/config.json`。

## Docker Compose

默认 `docker-compose.yml` 使用相对目录，便于公开部署和二次修改：

```yaml
volumes:
  - ./data:/data
  - ./strm:/strm
ports:
  - "${ANISTRM_PORT:-8080}:8080"
```

如果你的 Docker 环境需要指定 DNS，可以在本地部署时自行添加：

```yaml
services:
  anistrm:
    dns:
      - 你的_DNS_IP
```

如果要把 STRM 输出到宿主机媒体库目录，把 `./strm:/strm` 改成你的实际路径即可。

## 配置说明

页面会保存以下配置到 `/data/config.json`：

- `enabled`：是否启用定时任务。
- `full_sync_once`：下次运行时执行一次全量生成，运行后自动关闭。
- `use_proxy` / `http_proxy`：是否通过 HTTP/SOCKS 代理访问 Open ANi。
- `proxy_base`：Open ANi 入口，默认 `https://openani.an-i.workers.dev`。
- `selected_seasons`：要生成的季度，可包含 `latest` 或 `ANi`。
- `cron`：定时任务表达式，使用 5 段 crontab 格式。
- `output_dir`：容器内固定为 `/strm`。

## 说明

项目访问 Open ANi 目录接口时会发送一个空密码字段，这是该目录接口的兼容请求体，不包含任何账号或密钥。

本项目只生成本地媒体库索引用文件，不提供媒体内容本身。请遵守你所在地区的法律法规以及相关站点的使用规则。

## License

MIT
