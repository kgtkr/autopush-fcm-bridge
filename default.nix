{ lib, ... }: {
  perSystem = { config, system, pkgs, cargo2nix-ifd-lib, ... }:
    let
      rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
      projectName = "autopush-fcm-bridge";
      src = ./.;

      migratedPostgresData = pkgs.stdenv.mkDerivation {
        name = "migrated-postgres-data";
        src = ./migrations;
        nativeBuildInputs = [
          pkgs.postgresql
          pkgs.sqlx-cli
        ];
        dontFixup = true;
        buildPhase = ''
          export PGDATA="$TMPDIR/pgdata"
          export PGSOCKET="$TMPDIR/pgsocket"
          mkdir -p "$PGDATA" "$PGSOCKET"
          initdb -D "$PGDATA" --no-locale --encoding=UTF8 -U postgres --auth=trust
          pg_ctl -D "$PGDATA" -l "$TMPDIR/postgres.log" -o "-k $PGSOCKET -h \"\"" start
          export DATABASE_URL="postgresql://postgres@localhost/build_db?host=$PGSOCKET"
          sqlx database create
          sqlx migrate run --source "$src"
          pg_ctl -D "$PGDATA" stop
        '';
        installPhase = ''
          cp -r "$PGDATA" "$out"
        '';
      };

      filteredSrc = cargo2nix-ifd-lib.filterSrc {
        orFilter = path: _type:
          let
            parentDirs = [ "migrations" ];
          in
          pkgs.lib.any (dir: baseNameOf (dirOf path) == dir) parentDirs;
        inherit projectName src;
      };

      generatedSrc = cargo2nix-ifd-lib.generateSrc {
        src = filteredSrc;
        inherit projectName rustToolchain;
      };

      rustPkgs = pkgs.rustBuilder.makePackageSet {
        packageFun = import "${generatedSrc}/Cargo.nix";
        packageOverrides = pkgs: pkgs.rustBuilder.overrides.all ++
          [
            (pkgs.rustBuilder.rustLib.makeOverride {
              name = "autopush-fcm-bridge";
              overrideAttrs = drv:
                {
                  nativeBuildInputs = (drv.nativeBuildInputs or [ ]) ++ [ pkgs.postgresql ];
                  preBuild = ''
                    export PGDATA="$TMPDIR/pgdata"
                    export PGSOCKET="$TMPDIR/pgsocket"
                    mkdir -p "$PGSOCKET"
                    cp -r ${migratedPostgresData} "$PGDATA"
                    chmod -R 0700 "$PGDATA"
                    pg_ctl -D "$PGDATA" -l "$TMPDIR/postgres.log" -o "-k $PGSOCKET -h \"\"" start || {
                      echo "=== Postgres Log ==="
                      cat "$TMPDIR/postgres.log"
                      exit 1
                    }
                    export DATABASE_URL="postgresql://postgres@localhost/build_db?host=$PGSOCKET"
                  '';
                  postBuild = ''
                    pg_ctl -D "$PGDATA" stop
                  '';
                };
            })
            # https://github.com/launchbadge/sqlx/issues/2911 の問題でnix buildのときビルドしてしまうためworkaround
            (pkgs.rustBuilder.rustLib.makeOverride {
              name = "sqlx-sqlite";
              overrideAttrs = drv:
                {
                  propagatedBuildInputs = drv.propagatedBuildInputs or [ ] ++ [
                    pkgs.sqlite
                  ];
                  patchPhase = ''
                    echo "" > src/lib.rs
                  '';
                };
            })
          ];
        inherit rustToolchain;
        rustcLinkFlags = [ "--remap-path-prefix=${rustToolchain}=/rust" ];
        rustcBuildFlags = [ "--remap-path-prefix=${rustToolchain}=/rust" ];
      };
    in
    {
      packages = {
        autopush-fcm-bridge = (rustPkgs.workspace.autopush-fcm-bridge { }).bin;
        migratedPostgresData = migratedPostgresData;
        default = config.packages.autopush-fcm-bridge;
      };
    };
}
