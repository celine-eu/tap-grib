# CHANGELOG

<!-- version list -->

## v1.4.4 (2025-12-25)

### Bug Fixes

- Improve past filter, assert if is an instantaneous value
  ([`f4db871`](https://github.com/celine-eu/tap-grib/commit/f4db8719de4eadca17238f9328d7dd8b652ddb24))


## v1.4.3 (2025-11-30)

### Bug Fixes

- Review incremental keys handling. reorder PK definition
  ([`4af079d`](https://github.com/celine-eu/tap-grib/commit/4af079d87149018ad9f1d89f8bc8cffedc3e4f0c))


## v1.4.2 (2025-11-30)

### Bug Fixes

- Review state management
  ([`7e0dc2a`](https://github.com/celine-eu/tap-grib/commit/7e0dc2af9d0a3ef09d2e1fb4cf471ffe85249806))


## v1.4.1 (2025-11-29)

### Bug Fixes

- Bump
  ([`8f80b0f`](https://github.com/celine-eu/tap-grib/commit/8f80b0fa3956e04f67fc64264bd7530b997f30bf))

### Chores

- Add build in semantic release
  ([`bd2cbfe`](https://github.com/celine-eu/tap-grib/commit/bd2cbfe4a4475f289ab998bcb7558468d9071f64))


## v1.4.0 (2025-11-29)

### Features

- Add skip_past option
  ([`7efface`](https://github.com/celine-eu/tap-grib/commit/7efface553b210f2266c9f5d06cd2db5c6ab65b6))


## v1.3.2 (2025-11-25)

### Bug Fixes

- Use primary_keys value
  ([`cabf07d`](https://github.com/celine-eu/tap-grib/commit/cabf07dc1b7d123f20d12e1d2697299928ee6183))

### Chores

- Updpate uv.sync
  ([`178ab87`](https://github.com/celine-eu/tap-grib/commit/178ab87f13b726e40462bfeba538140bb429c8aa))


## v1.3.1 (2025-11-24)

### Bug Fixes

- Missing return on bbox check
  ([`e44964d`](https://github.com/celine-eu/tap-grib/commit/e44964d35b29ac1cfda7ee76aef86c21e1fd9191))

### Chores

- Update uv.sync
  ([`17141e0`](https://github.com/celine-eu/tap-grib/commit/17141e020eb134ee422511dd550b53a7f30ae39c))


## v1.3.0 (2025-11-24)

### Features

- Add base datetime, forecast time and unit
  ([`97e08f6`](https://github.com/celine-eu/tap-grib/commit/97e08f6fc204b4950ecb0479cad054f5d8a590cb))


## v1.2.0 (2025-11-10)

### Chores

- Up lock
  ([`203b3af`](https://github.com/celine-eu/tap-grib/commit/203b3af002882b530b359dd333acf9205f995229))

- Update lock
  ([`ba720a9`](https://github.com/celine-eu/tap-grib/commit/ba720a9a12eb16427dc8a39f576f2c15c5233054))

- **deps**: Bump boto3 from 1.39.11 to 1.40.64
  ([`435b7a4`](https://github.com/celine-eu/tap-grib/commit/435b7a482cad89059cf0d60a155a370e75641cd7))

- **deps**: Bump meltano from 3.9.1 to 4.0.4
  ([`0616881`](https://github.com/celine-eu/tap-grib/commit/061688150bf8cfb977f30a0a9786980437027276))

- **deps**: Bump s3fs from 2025.9.0 to 2025.10.0
  ([`b81441d`](https://github.com/celine-eu/tap-grib/commit/b81441da87a7edbfc84f8c77c8667240699eaefb))

- **deps**: Bump types-requests from 2.32.4.20250809 to 2.32.4.20250913
  ([`8c8a3a0`](https://github.com/celine-eu/tap-grib/commit/8c8a3a0ac0d203aee6557a286c28becde260e3da))

- **deps**: Update singer-sdk[faker] requirement
  ([`90aa727`](https://github.com/celine-eu/tap-grib/commit/90aa7279272db922d7c75b5bd012174323218058))

### Features

- Support for bboxes as array
  ([`5e8812e`](https://github.com/celine-eu/tap-grib/commit/5e8812e04a4b0ad7fb20e2fac10777df97e335b1))


## v1.1.0 (2025-11-06)

### Features

- Add bbox filtering
  ([`4f6d3ce`](https://github.com/celine-eu/tap-grib/commit/4f6d3ce49e86a64620dafe0560f9c325b1716884))


## v1.0.1 (2025-11-03)

### Bug Fixes

- Better type handling in storage
  ([`6e7f2d2`](https://github.com/celine-eu/tap-grib/commit/6e7f2d23e0e41599050859a8965b3ede5c6a5c38))

### Chores

- Add boto3
  ([`e506303`](https://github.com/celine-eu/tap-grib/commit/e506303578d8ff8cdbb6693b19a09579acaa758c))

- Add dev env to ty
  ([`b3754ee`](https://github.com/celine-eu/tap-grib/commit/b3754eed938bd14569f62f97a1ff72cc83214121))

- Add dev env to ty
  ([`526e9c8`](https://github.com/celine-eu/tap-grib/commit/526e9c8fb28bf49874f765701ddbe0cace878040))

- Drop older py
  ([`9059ee6`](https://github.com/celine-eu/tap-grib/commit/9059ee6ad71ac628958273a7a757c0fc4f23d1f2))

- Fix mypy checks
  ([`a05ab6f`](https://github.com/celine-eu/tap-grib/commit/a05ab6fabb476d7f81d8294ddd83e19ca5956d06))

- Update deps
  ([`9e1ef44`](https://github.com/celine-eu/tap-grib/commit/9e1ef44d7ec4997ffd5b3a5e3b00a5320949d04b))

- **deps**: Bump the runtime-dependencies group across 1 directory with 2 updates
  ([`94705ee`](https://github.com/celine-eu/tap-grib/commit/94705eed2f70e9be1f6af264d4a3579b26930168))

### Continuous Integration

- Bump the actions group across 1 directory with 5 updates
  ([`b4844de`](https://github.com/celine-eu/tap-grib/commit/b4844de3fe2fb7d93f31d662e83e894f65fd0723))


## v1.0.0 (2025-11-03)

- Initial Release
