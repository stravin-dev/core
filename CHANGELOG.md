## [1.9.6](https://github.com/stravin-dev/core/compare/v1.9.5...v1.9.6) (2026-11-24)


### Bug Fixes

* **node,edge,local:** bound pack_cache memory, log & retry transient info/refs 502s, and seed multi-repo local bootstrap ([d1996c8](https://github.com/stravin-dev/core/commit/d1996c86615e82151b22568731c78159407db821)), closes [#160](https://github.com/stravin-dev/core/issues/160)
* **node,edge,local:** bound pack_cache memory, log & retry transient info/refs 502s, and seed multi-repo local bootstrap ([#162](https://github.com/stravin-dev/core/issues/162)) ([e71d162](https://github.com/stravin-dev/core/commit/e71d16232aeed3d9868d51685431c027e7b3ad3a))

## [1.9.5](https://github.com/stravin-dev/core/compare/v1.9.4...v1.9.5) (2026-11-24)


### Performance Improvements

* **pack,loadgen:** drain multi-batch Tier-0 compaction, restore pack.compression=1, and remove redundant loadgen git subprocesses ([7134286](https://github.com/stravin-dev/core/commit/7134286a2a71ed886ba5301ddd7f6f5a8474e7cc))
* **pack,loadgen:** drain multi-batch Tier-0 compaction, restore pack.compression=1, and remove redundant loadgen git subprocesses ([#158](https://github.com/stravin-dev/core/issues/158)) ([6501d54](https://github.com/stravin-dev/core/commit/6501d547a1f8987575b688675391bf23edae5218))

## [1.9.4](https://github.com/stravin-dev/core/compare/v1.9.3...v1.9.4) (2026-11-24)


### Performance Improvements

* **node,pack:** pure-Go v2 ls-refs, reduce compaction/MIDX churn, and full-clone pack cache ([3f2326a](https://github.com/stravin-dev/core/commit/3f2326aa9feae7ad69136fe33a7f67deb0bfceb5))
* **node,pack:** pure-Go v2 ls-refs, reduce compaction/MIDX churn, and full-clone pack cache ([#154](https://github.com/stravin-dev/core/issues/154)) ([#155](https://github.com/stravin-dev/core/issues/155)) ([2dd5074](https://github.com/stravin-dev/core/commit/2dd5074600bd1818c39ee084aeb5534a694bcb98))

## [1.9.3](https://github.com/stravin-dev/core/compare/v1.9.2...v1.9.3) (2026-11-24)


### Performance Improvements

* **ddb,node:** add linearizable META ref_version/manifest_version and version-gated in-memory cache ([#153](https://github.com/stravin-dev/core/issues/153)) ([d0bfd47](https://github.com/stravin-dev/core/commit/d0bfd476ddbf40eee95a749d5cf130a6f926d682))
* **ddb,node:** add linearizable META versioning and version-gated in-memory cache ([0d92a40](https://github.com/stravin-dev/core/commit/0d92a407328df302caf2a36d1f99c31bad069511))

## [1.9.2](https://github.com/stravin-dev/core/compare/v1.9.1...v1.9.2) (2026-11-24)


### Performance Improvements

* **node:** cut redundant DDB/S3/git overhead in upload-pack, SyncAndPin, and receive-pack ([aa04d89](https://github.com/stravin-dev/core/commit/aa04d89f23d02f940973dbd15858f037e97ef7b7))
* **node:** cut redundant DDB/S3/git overhead in upload-pack, SyncAndPin, and receive-pack ([#149](https://github.com/stravin-dev/core/issues/149)) ([ef7ecde](https://github.com/stravin-dev/core/commit/ef7ecded15dd7ceea73facad1a7cbf56bb00ea19))

## [1.9.1](https://github.com/stravin-dev/core/compare/v1.9.0...v1.9.1) (2026-11-24)


### Bug Fixes

* **docker:** bump runtime base image to alpine:3.22 (Git 2.49.1) ([ab226e8](https://github.com/stravin-dev/core/commit/ab226e89f20ddcdf97d79fb27e6c1e097b1369eb))
* **docker:** bump runtime base image to alpine:3.22 (Git 2.49.1) ([#146](https://github.com/stravin-dev/core/issues/146)) ([5adb649](https://github.com/stravin-dev/core/commit/5adb649c7e58de1aee9eed35b00ec7e0b5814bf1))

# [1.9.0](https://github.com/stravin-dev/core/compare/v1.8.1...v1.9.0) (2026-11-24)


### Features

* **node,gitexec:** add configurable upload-pack concurrency and pack-objects tuning ([8d71127](https://github.com/stravin-dev/core/commit/8d71127f65ec3a2b896e3034ad7a0b6ec59cc990))
* **node,gitexec:** add configurable upload-pack concurrency and pack-objects tuning ([#144](https://github.com/stravin-dev/core/issues/144)) ([b716fc6](https://github.com/stravin-dev/core/commit/b716fc6a6a29614713b7f00588447b7e2cbde82b))

## [1.8.1](https://github.com/stravin-dev/core/compare/v1.8.0...v1.8.1) (2026-11-23)


### Bug Fixes

* **node,cache:** build MIDX/bitmap via --stdin-packs and run refreshAuxIndexes asynchronously ([ca98647](https://github.com/stravin-dev/core/commit/ca98647c19fd20851a32b4b93ff609dbd326946a))
* **node,cache:** build MIDX/bitmap via --stdin-packs and run refreshAuxIndexes asynchronously ([#142](https://github.com/stravin-dev/core/issues/142)) ([be26232](https://github.com/stravin-dev/core/commit/be26232c6eeaf747321f9fdcf91eca6927ff10ed))

# [1.8.0](https://github.com/stravin-dev/core/compare/v1.7.2...v1.8.0) (2026-11-23)


### Features

* **observability:** add NVMe cache pruning and aux-index failure metrics to stravin-core dashboard ([bc9fc1f](https://github.com/stravin-dev/core/commit/bc9fc1f60ae557222d092391e6dde43590ec3b18))
* **observability:** add NVMe cache pruning and aux-index failure metrics to stravin-core dashboard ([#140](https://github.com/stravin-dev/core/issues/140)) ([c48872a](https://github.com/stravin-dev/core/commit/c48872aab87d39d863f8c84e939d3067ec661c02))

## [1.7.2](https://github.com/stravin-dev/core/compare/v1.7.1...v1.7.2) (2026-11-23)


### Bug Fixes

* **cache:** prune superseded packs via epoch tracking without waiting for pins == 0 ([845ca9f](https://github.com/stravin-dev/core/commit/845ca9fd2c587b5b852f92d4009189bbf98421f9))
* **cache:** prune superseded packs via epoch tracking without waiting for pins == 0 ([#138](https://github.com/stravin-dev/core/issues/138)) ([8e9a398](https://github.com/stravin-dev/core/commit/8e9a39883e6e27626f172caee409125e22bf53db))
* **loadgen:** classify stale old_oid Ref-CAS rejection as retryable_conflict ([de3a384](https://github.com/stravin-dev/core/commit/de3a38484d154699f0dcd477c90861c7705c5584))
* **node:** clean up stale multi-pack-index and commit-graph locks in refreshAuxIndexes ([af23ef9](https://github.com/stravin-dev/core/commit/af23ef914085277fda14cecfa94a331b446ed122))
* **observability:** scope Overview error metrics to selected Grafana time range on both dashboards ([ff4edf9](https://github.com/stravin-dev/core/commit/ff4edf9e1850415d053f6102513a65e5da228d0c))

## [1.7.1](https://github.com/stravin-dev/core/compare/v1.7.0...v1.7.1) (2026-11-23)


### Bug Fixes

* **loadgen:** prevent SSH hangs, enforce per-command git timeout, and skip SSH scenarios without --ssh-key ([6448fe5](https://github.com/stravin-dev/core/commit/6448fe58a27087d1ae365f5a82ed532c90680efd))
* **loadgen:** prevent SSH hangs, enforce per-command git timeout, and skip SSH scenarios without --ssh-key ([#130](https://github.com/stravin-dev/core/issues/130)) ([12ef4d4](https://github.com/stravin-dev/core/commit/12ef4d426ef6ae5034354b90ba3de876c07be3d6))
* **node,cache:** eliminate stale/missing ref race in SyncAndPin and singleflight coalescing ([4b4b809](https://github.com/stravin-dev/core/commit/4b4b809d2b28cfc36eca328ddb5808ff62522262))

# [1.7.0](https://github.com/stravin-dev/core/compare/v1.6.0...v1.7.0) (2026-11-23)


### Features

* **observability:** add high-level Overview health section to stravin-core Grafana dashboard ([c052be1](https://github.com/stravin-dev/core/commit/c052be1f869b9c4707e8f13044bc50afce5e2094)), closes [hi#level](https://github.com/hi/issues/level)
* **observability:** add high-level Overview health section to stravin-core Grafana dashboard ([#126](https://github.com/stravin-dev/core/issues/126)) ([dc7b251](https://github.com/stravin-dev/core/commit/dc7b251a93712fa50923173ed74e4366ff55c359)), closes [hi#level](https://github.com/hi/issues/level)

# [1.6.0](https://github.com/stravin-dev/core/compare/v1.5.0...v1.6.0) (2026-11-23)


### Bug Fixes

* **cache:** invalidate multi-pack-index and bitmap when unlinking pruned packs ([6893156](https://github.com/stravin-dev/core/commit/68931564f6332227ba3f529ab2cee6610d8818bb))


### Features

* **node,storage:** adopt reftable NVMe cache, MIDX/bitmap/commit-graph, and bundle-uri offload ([f9fb3aa](https://github.com/stravin-dev/core/commit/f9fb3aa0adef72be89bbdb3c64ffd5f07687b06c))
* **node,storage:** adopt reftable NVMe cache, MIDX/bitmap/commit-graph, and bundle-uri offload ([#124](https://github.com/stravin-dev/core/issues/124)) ([b00e6d7](https://github.com/stravin-dev/core/commit/b00e6d7cb85cdb15c17eed816203c5868ade9a74))

# [1.5.0](https://github.com/stravin-dev/core/compare/v1.4.0...v1.5.0) (2026-11-23)


### Features

* **edge,node:** support Git Wire Protocol v2 and end-to-end SHA-256 transport ([8483dbf](https://github.com/stravin-dev/core/commit/8483dbfb3963b43621a1cb2b14345c06a8888d19))
* **edge,node:** support Git Wire Protocol v2 and end-to-end SHA-256 transport ([#123](https://github.com/stravin-dev/core/issues/123)) ([b033a29](https://github.com/stravin-dev/core/commit/b033a29e117d03f3a61e992ac93970edab7c52a5))

# [1.4.0](https://github.com/stravin-dev/core/compare/v1.3.0...v1.4.0) (2026-11-23)


### Features

* **pack:** add dual-hash SHA-1/SHA-256 engine, DDB object format metadata, and Wasm ABI support ([1403c0a](https://github.com/stravin-dev/core/commit/1403c0a0f93fde14985b6e32523a49af7ca83da9))
* **pack:** add dual-hash SHA-1/SHA-256 engine, DDB object format metadata, and Wasm ABI support ([#122](https://github.com/stravin-dev/core/issues/122)) ([067e528](https://github.com/stravin-dev/core/commit/067e528888e95a9aa909b0d4fd3cd162ca48b16a))

# [1.3.0](https://github.com/stravin-dev/core/compare/v1.2.1...v1.3.0) (2026-11-23)


### Features

* **gitexec:** enforce Git 3.0 explicit --git-dir guardrails and compatibility tests ([fe64013](https://github.com/stravin-dev/core/commit/fe6401374574984d35d15defbadb95df777b198d))
* **gitexec:** enforce Git 3.0 explicit --git-dir guardrails and compatibility tests ([#120](https://github.com/stravin-dev/core/issues/120)) ([6d13b2e](https://github.com/stravin-dev/core/commit/6d13b2e080aac746dc6ca6c4f7c4bdcf26035014))
* **observability:** add dev/agent granularity, scenario pie charts, and collapsible rows to loadgen dashboard ([362c82b](https://github.com/stravin-dev/core/commit/362c82b36a9bfea2bf578c87a4cedd5013c0c785))
* **observability:** add dev/agent granularity, scenario pie charts, and collapsible rows to loadgen dashboard ([#121](https://github.com/stravin-dev/core/issues/121)) ([47864cf](https://github.com/stravin-dev/core/commit/47864cfbc2c9cd290b7ccb398027fa735aac00fc))

## [1.2.1](https://github.com/stravin-dev/core/compare/v1.2.0...v1.2.1) (2026-11-23)


### Bug Fixes

* **edge:** classify client disconnects as HTTP 499 client_canceled ([ba59700](https://github.com/stravin-dev/core/commit/ba59700de011d06734c49413518e304e4aaa3ed5))
* **edge:** classify client disconnects as HTTP 499 client_canceled ([#113](https://github.com/stravin-dev/core/issues/113)) ([3bae825](https://github.com/stravin-dev/core/commit/3bae825eb16efe057e91d9e590f0c14dc10eb772))
* **gitexec:** enable uploadpack.allowReachableSHA1InWant ([287d9f5](https://github.com/stravin-dev/core/commit/287d9f562b0d4aebea37705f32b7b0e27a645bc7))
* **gitexec:** enable uploadpack.allowReachableSHA1InWant ([#110](https://github.com/stravin-dev/core/issues/110)) ([#112](https://github.com/stravin-dev/core/issues/112)) [skip ci] ([655d501](https://github.com/stravin-dev/core/commit/655d501a91601ff5a3d4b261bf191d0e94da78e6))

# [1.2.0](https://github.com/stravin-dev/core/compare/v1.1.1...v1.2.0) (2026-11-23)


### Features

* **ci:** package release artifacts as tar.zst with zstd --long ([d4d4a8b](https://github.com/stravin-dev/core/commit/d4d4a8bdb3e93ac788c44adaa753d9768305511c))
* **ci:** package release artifacts as tar.zst with zstd --long ([#109](https://github.com/stravin-dev/core/issues/109)) ([a0add5a](https://github.com/stravin-dev/core/commit/a0add5ac2914d546c5c9bf1e0958578e63808ed7))

## [1.1.1](https://github.com/stravin-dev/core/compare/v1.1.0...v1.1.1) (2026-11-23)


### Bug Fixes

* prune superseded NVMe cache packs and cap Tier-0 compaction ([e034d37](https://github.com/stravin-dev/core/commit/e034d372cc96cd13c5126dee195285995203a8d3)), closes [#104](https://github.com/stravin-dev/core/issues/104) [#105](https://github.com/stravin-dev/core/issues/105)
* prune superseded NVMe cache packs and cap Tier-0 compaction ([#107](https://github.com/stravin-dev/core/issues/107)) ([cae3bc9](https://github.com/stravin-dev/core/commit/cae3bc90e0538571221990ea6becc5ef728ea8d4))

# [1.1.0](https://github.com/stravin-dev/core/compare/v1.0.0...v1.1.0) (2026-11-23)


### Bug Fixes

* add make asdf and ignore bin directory ([cc6ab0d](https://github.com/stravin-dev/core/commit/cc6ab0d27a30056082c0c5cfffd0968e39a99b55))
* **ci:** grant semver-dry-run contents:write and deflake gitops fsck test ([c1fe94e](https://github.com/stravin-dev/core/commit/c1fe94e2aeef66c5ecbc98c21e499ebed58ac4db))
* **ci:** grant semver-dry-run contents:write and deflake gitops fsck test ([#108](https://github.com/stravin-dev/core/issues/108)) ([f7ded4b](https://github.com/stravin-dev/core/commit/f7ded4b69b6d853b661cdd9f8d063b21c980d96c))
* **ctrl:** require bearer auth on /v1/* admin API and bind loopback by default ([#48](https://github.com/stravin-dev/core/issues/48)) ([7491df3](https://github.com/stravin-dev/core/commit/7491df3747a497bcb66623cba639ecc1f9347a6d))
* **ctrl:** require bearer auth on /v1/* admin API and bind loopback by default ([#48](https://github.com/stravin-dev/core/issues/48)) ([#63](https://github.com/stravin-dev/core/issues/63)) ([6cfa7b0](https://github.com/stravin-dev/core/commit/6cfa7b09f253acb42dfc4a95b6cf1b9e96f1262a))
* **edge:** decompress Content-Encoding: gzip request bodies in Smart HTTP handler ([#96](https://github.com/stravin-dev/core/issues/96)) ([#100](https://github.com/stravin-dev/core/issues/100)) ([cf041d2](https://github.com/stravin-dev/core/commit/cf041d2acf1bdf6d0302e9b0baf2a05e92abcd1b))
* **edge:** decompress Content-Encoding: gzip Smart HTTP request bodies ([#96](https://github.com/stravin-dev/core/issues/96)) ([00650ae](https://github.com/stravin-dev/core/commit/00650ae5e15504068cfae526a3cee08ee544d9a9))
* **edge:** remove unauthenticated /metrics HTTP endpoint in favor of OTel OTLP ([#55](https://github.com/stravin-dev/core/issues/55)) ([ae3c03e](https://github.com/stravin-dev/core/commit/ae3c03e40789303986f1b1f8990549a554d4856c))
* **edge:** remove unauthenticated /metrics HTTP endpoint in favor of OTel OTLP ([#55](https://github.com/stravin-dev/core/issues/55)) ([#59](https://github.com/stravin-dev/core/issues/59)) ([34eaea8](https://github.com/stravin-dev/core/commit/34eaea87dc496f5fbef03cdd441ff03964b15d12))
* **gitexec,node,pack:** enable Linux sandbox and timeout defaults ([#52](https://github.com/stravin-dev/core/issues/52)) ([4462c92](https://github.com/stravin-dev/core/commit/4462c92337a184e0a3a5a1436d773b1025f71f89))
* **gitexec,node,pack:** enable Linux sandbox and timeout defaults ([#52](https://github.com/stravin-dev/core/issues/52)) ([#67](https://github.com/stravin-dev/core/issues/67)) ([38bbc1b](https://github.com/stravin-dev/core/commit/38bbc1bf093cf2b11b248c7911bd7616d6b85a13))
* **hooks:** block private, loopback, link-local, CGNAT, and proxy SSRF in Tier 2 webhooks ([#47](https://github.com/stravin-dev/core/issues/47)) ([6d1a327](https://github.com/stravin-dev/core/commit/6d1a327b2cf2d66a77eaba2b14fb478ec0398e83))
* **hooks:** block private/loopback/proxy SSRF in Tier 2 webhook dispatcher ([#47](https://github.com/stravin-dev/core/issues/47)) ([#60](https://github.com/stravin-dev/core/issues/60)) ([d8614cc](https://github.com/stravin-dev/core/commit/d8614cce6fa03f98219b6e37a0e99995315208ea))
* **loadgen:** kill git process group on context cancel to prevent hang ([#86](https://github.com/stravin-dev/core/issues/86)) ([ffb10b1](https://github.com/stravin-dev/core/commit/ffb10b11abe2a761ce7754bf58bec4cf1e3aaaa9))
* **loadgen:** kill git process group on context cancel to prevent hang ([#86](https://github.com/stravin-dev/core/issues/86)) ([#87](https://github.com/stravin-dev/core/issues/87)) ([8fed245](https://github.com/stravin-dev/core/commit/8fed24513d771cc446ae3c21c64c1d2d29cd56ae))
* **node,pack,ctrl,s3:** wire KMS envelope encryption into S3 stores ([#50](https://github.com/stravin-dev/core/issues/50)) ([33f692a](https://github.com/stravin-dev/core/commit/33f692a54d2204d682299188bc970c4a22efa293))
* **node,pack,ctrl,s3:** wire KMS envelope encryption into S3 stores ([#50](https://github.com/stravin-dev/core/issues/50)) ([#70](https://github.com/stravin-dev/core/issues/70)) ([0874189](https://github.com/stravin-dev/core/commit/0874189f83911268196c2833884068a6a455fbc1))
* **node:** enforce streaming caps in ParseReceiveCommands ([#51](https://github.com/stravin-dev/core/issues/51)) ([#62](https://github.com/stravin-dev/core/issues/62)) ([69ceb3a](https://github.com/stravin-dev/core/commit/69ceb3a7f485d7cb8e50cb7fcd394fe4196830e7))
* **node:** enforce streaming command count and ref/line caps in ParseReceiveCommands ([#51](https://github.com/stravin-dev/core/issues/51)) ([d13c7e4](https://github.com/stravin-dev/core/commit/d13c7e44aa855f8cebce51d4dbdce79d8314eb51))
* **node:** harden ref name validation and reject ref/directory collisions ([#53](https://github.com/stravin-dev/core/issues/53)) ([54a90a0](https://github.com/stravin-dev/core/commit/54a90a065c9f1e5437a84e53cf0d585b5baf1e79))
* **node:** harden ref name validation and reject ref/directory collisions ([#53](https://github.com/stravin-dev/core/issues/53)) ([#64](https://github.com/stravin-dev/core/issues/64)) ([646226b](https://github.com/stravin-dev/core/commit/646226b8feeb011bf4ef9133a9e9d0c5363f24a9))
* **node:** sanitize non-UTF-8 git stderr on OTel spans and record non-zero exit errors ([#98](https://github.com/stravin-dev/core/issues/98)) ([e9884a5](https://github.com/stravin-dev/core/commit/e9884a5d330a9c62ae04a8b791fadd32e2ec90b5))
* **node:** sanitize non-UTF-8 git stderr on OTel spans and record non-zero exit errors ([#98](https://github.com/stravin-dev/core/issues/98)) ([#101](https://github.com/stravin-dev/core/issues/101)) ([2100650](https://github.com/stravin-dev/core/commit/2100650b2da502e90ee4baa0d81bc961659a27db))
* **pack,wasm:** bound DiffTree recursion, DAG visits, limit, and context ([#54](https://github.com/stravin-dev/core/issues/54)) ([e06b23b](https://github.com/stravin-dev/core/commit/e06b23bed7d3a916e71bf144131bda90127ea208))
* **pack,wasm:** enforce DiffTree depth, DAG, limit, and context bounds ([#54](https://github.com/stravin-dev/core/issues/54)) ([#61](https://github.com/stravin-dev/core/issues/61)) ([d73488b](https://github.com/stravin-dev/core/commit/d73488bcd01a4c5942d3977aa25b37fd2da632e1))
* **rpc,node,edge:** enforce gRPC GitWire auth interceptor and optional TLS/mTLS ([#49](https://github.com/stravin-dev/core/issues/49)) ([fc2cdc1](https://github.com/stravin-dev/core/commit/fc2cdc135b5373d09565679fcaaea1e0d8eeb117))
* **rpc,node,edge:** enforce gRPC GitWire auth interceptor and optional TLS/mTLS ([#49](https://github.com/stravin-dev/core/issues/49)) ([#65](https://github.com/stravin-dev/core/issues/65)) ([8c52157](https://github.com/stravin-dev/core/commit/8c52157aa3ea7385003f79001c3a71013935b707))
* **telemetry:** configure explicit second-scale bucket boundaries for duration histograms ([#97](https://github.com/stravin-dev/core/issues/97)) ([ddf2bf6](https://github.com/stravin-dev/core/commit/ddf2bf634bd0963a566a6a8ca2088b9b6097a24d))
* **telemetry:** configure explicit second-scale bucket boundaries for duration histograms ([#99](https://github.com/stravin-dev/core/issues/99)) ([7057579](https://github.com/stravin-dev/core/commit/7057579c02fc53f686ef22b5330315ddfe75c269))


### Features

* **chaos:** add local load+chaos verification suite and README guide ([#76](https://github.com/stravin-dev/core/issues/76)) ([8e0b770](https://github.com/stravin-dev/core/commit/8e0b7704b0eb9910582f5b5fef5e0f445f5ece4e))
* **chaos:** add Toxiproxy chaos Compose stack & stravin-chaos nemesis ([#74](https://github.com/stravin-dev/core/issues/74)) ([#78](https://github.com/stravin-dev/core/issues/78)) ([b74fb45](https://github.com/stravin-dev/core/commit/b74fb45c55e846238825c38c4e02d0237bc55655))
* **chaos:** add Toxiproxy chaos Compose stack and stravin-chaos nemesis ([#74](https://github.com/stravin-dev/core/issues/74)) ([9efe0e1](https://github.com/stravin-dev/core/commit/9efe0e1b30d230b96f5635952234364a25e39353))
* **chaos:** local load + chaos verification suite (make load-test, make chaos-e2e) & README guide ([#76](https://github.com/stravin-dev/core/issues/76)) ([#82](https://github.com/stravin-dev/core/issues/82)) ([1789031](https://github.com/stravin-dev/core/commit/1789031234c6bd89af41ead54dae288ae3b49b21))
* **ci:** add npx semantic-release pipeline and release artifact builds ([ba27c1e](https://github.com/stravin-dev/core/commit/ba27c1e2a4e9c1701a02ba6ea440fdd37228446f))
* **ci:** add npx semantic-release pipeline and release artifact builds ([#102](https://github.com/stravin-dev/core/issues/102)) ([#103](https://github.com/stravin-dev/core/issues/103)) ([9d4854f](https://github.com/stravin-dev/core/commit/9d4854ffad6728234226273a2009ac1d8c059303))
* **compose:** raise local stravin-edge rate limits for stress testing ([#88](https://github.com/stravin-dev/core/issues/88)) ([3d6832e](https://github.com/stravin-dev/core/commit/3d6832e861ce0b40816d27f4ec300ae0ca9dead7))
* **compose:** raise local stravin-edge rate limits for stress testing ([#88](https://github.com/stravin-dev/core/issues/88)) ([#89](https://github.com/stravin-dev/core/issues/89)) ([48859a7](https://github.com/stravin-dev/core/commit/48859a7d419ec357c2565a2d09368d29dfe257d0))
* **ctrl:** add automated orphaned-pack recovery branch reconciler ([#58](https://github.com/stravin-dev/core/issues/58)) ([94aba62](https://github.com/stravin-dev/core/commit/94aba62b826ad1013934464d910d3c0c39343a04))
* **ctrl:** add automated orphaned-pack recovery branch reconciler ([#58](https://github.com/stravin-dev/core/issues/58)) ([#69](https://github.com/stravin-dev/core/issues/69)) ([8023a0a](https://github.com/stravin-dev/core/commit/8023a0aac90a0b5f2e8499fa3b6a5438680c59a8))
* **ctrl:** guard failover promotion with witness quorum and operator gate ([#57](https://github.com/stravin-dev/core/issues/57)) ([3b98025](https://github.com/stravin-dev/core/commit/3b98025776a992dc35a11a33a243ad1f51fc2169))
* **ctrl:** guard failover promotion with witness quorum and operator gate ([#57](https://github.com/stravin-dev/core/issues/57)) ([#68](https://github.com/stravin-dev/core/issues/68)) ([c83574e](https://github.com/stravin-dev/core/commit/c83574e24951498f4167abcdcfd9e6a3e630c3f4))
* **health:** add Kubernetes /livez, /readyz, /startupz probes and Docker HEALTHCHECKs ([#84](https://github.com/stravin-dev/core/issues/84)) ([f1a54bc](https://github.com/stravin-dev/core/commit/f1a54bc753000147775e04a9b04a9202d1a2bb27))
* **health:** add Kubernetes /livez, /readyz, /startupz probes and Docker HEALTHCHECKs ([#84](https://github.com/stravin-dev/core/issues/84)) ([#85](https://github.com/stravin-dev/core/issues/85)) ([4a3bdb1](https://github.com/stravin-dev/core/commit/4a3bdb1b51e5582adfdfb815a3a933d562dc1805))
* **loadgen:** add 22 dev & agent Git scenarios with deterministic entropy ([#75](https://github.com/stravin-dev/core/issues/75)) ([d8345cf](https://github.com/stravin-dev/core/commit/d8345cfcaaab0910dc42c30c2fe5e2fc18be837d))
* **loadgen:** add 22 dev & agent Git scenarios with deterministic entropy ([#75](https://github.com/stravin-dev/core/issues/75)) ([#80](https://github.com/stravin-dev/core/issues/80)) ([6ad9a69](https://github.com/stravin-dev/core/commit/6ad9a69ec35bff181a67c243bda97199dd76630c))
* **loadgen:** add standalone stravin-loadgen binary, CLI, oracle & OTel flush ([#72](https://github.com/stravin-dev/core/issues/72)) ([3d54e2d](https://github.com/stravin-dev/core/commit/3d54e2d07f69f483393212ce7a342b96052c49b2))
* **loadgen:** add weighted random file sizes up to 512KB ([#90](https://github.com/stravin-dev/core/issues/90)) ([0c0b32f](https://github.com/stravin-dev/core/commit/0c0b32fff0ad85c884027cba901cfbd0859d16da))
* **loadgen:** add weighted random file sizes up to 512KB ([#90](https://github.com/stravin-dev/core/issues/90)) ([#93](https://github.com/stravin-dev/core/issues/93)) ([2dea8ad](https://github.com/stravin-dev/core/commit/2dea8ad3f607f631132033bf08c95152fd81332b))
* **loadgen:** standalone stravin-loadgen binary, CLI, oracle & OTel flush ([#72](https://github.com/stravin-dev/core/issues/72)) ([#79](https://github.com/stravin-dev/core/issues/79)) ([5c4f652](https://github.com/stravin-dev/core/commit/5c4f652f2c60498d13b6154eae33c3d03e52e629))
* **observability:** add cross-linked Grafana Core and Load Tester dashboards ([#73](https://github.com/stravin-dev/core/issues/73)) ([7411019](https://github.com/stravin-dev/core/commit/7411019a4c2fd30baa9631d55208dbf01ebf4a22))
* **observability:** add cross-linked Grafana Stravin Core & Load Tester dashboards ([#73](https://github.com/stravin-dev/core/issues/73)) ([#77](https://github.com/stravin-dev/core/issues/77)) ([9c92dca](https://github.com/stravin-dev/core/commit/9c92dca2aa7602a4341e4790c2a75af36f8ffe45))
* **observability:** expand Stravin Core Grafana dashboard with collapsible per-layer rows ([#91](https://github.com/stravin-dev/core/issues/91)) ([e8c96b5](https://github.com/stravin-dev/core/commit/e8c96b51d847abe8f5ab5a05884e8b452e509520))
* **observability:** expand Stravin Core Grafana dashboard with collapsible per-layer rows ([#91](https://github.com/stravin-dev/core/issues/91)) ([#95](https://github.com/stravin-dev/core/issues/95)) ([78cb838](https://github.com/stravin-dev/core/commit/78cb83831505902be2823ee686e9af179e613d9e))
* **pack,s3:** gate delayed GC deletion on S3 CRR ReplicationStatus and lag circuit breaker ([#56](https://github.com/stravin-dev/core/issues/56)) ([7ea1533](https://github.com/stravin-dev/core/commit/7ea153358fc4bfdfc86efa3707054ef1fbda6be3))
* **pack,s3:** gate delayed GC deletion on S3 CRR ReplicationStatus and lag circuit breaker ([#56](https://github.com/stravin-dev/core/issues/56)) ([#66](https://github.com/stravin-dev/core/issues/66)) ([cea66c8](https://github.com/stravin-dev/core/commit/cea66c8e21d22816463424aea86f6a6fc1965bea))
* **telemetry:** flush traces, metrics, and logs on shutdown across all binaries ([#81](https://github.com/stravin-dev/core/issues/81)) ([5c45c61](https://github.com/stravin-dev/core/commit/5c45c6199e5f618c50787f045bad2c3f36752c32))
* **telemetry:** flush traces, metrics, and logs on shutdown across all binaries ([#81](https://github.com/stravin-dev/core/issues/81)) ([#83](https://github.com/stravin-dev/core/issues/83)) ([04413f6](https://github.com/stravin-dev/core/commit/04413f618598c8b3954626871751ea917016d154))
