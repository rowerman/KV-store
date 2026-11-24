# RaftKV

## 构建

先加载共享开发环境：

```bash
source /home/devtools/env.sh
```

常用构建命令：

```bash
cmake --preset dev
cmake --build --preset dev
ctest --preset dev
```

其他配置：

```bash
cmake --preset release
cmake --preset asan
cmake --preset tsan
```

构建依赖由根目录 `vcpkg.json` 声明，并统一从共享 vcpkg 根读取。普通配置不会
自动修改共享依赖树；依赖增删由管理员显式执行 vcpkg 安装。

## 文档

模块职责和依赖边界见 [`docs/README.md`](docs/README.md)。

