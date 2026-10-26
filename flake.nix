{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    make-shell.url = "github:nicknovitski/make-shell";
    rust-overlay.url = "github:oxalica/rust-overlay";
    cargo2nix = {
      url = "github:cargo2nix/cargo2nix/release-0.12";
      inputs.rust-overlay.follows = "rust-overlay";
    };
    cargo2nix-ifd = {
      url = "github:kgtkr/cargo2nix-ifd";
      inputs.cargo2nix.follows = "cargo2nix";
    };
  };

  outputs = inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" "x86_64-darwin" ];
      imports = [
        inputs.make-shell.flakeModules.default
        ./default.nix
        ./image.nix
      ];
      perSystem = { self', system, ... }:
        let
          pkgs = import inputs.nixpkgs {
            inherit system;
            overlays = [
              inputs.cargo2nix.overlays.default
              (import inputs.rust-overlay)
            ];
          };
          cargo2nix-ifd-lib = inputs.cargo2nix-ifd.mkLib pkgs;
          rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
        in
        {
          _module.args = {
            inherit pkgs cargo2nix-ifd-lib;
          };
          make-shells.default = {
            env = {
              RUST_SRC_PATH = "${rustToolchain}/lib/rustlib/src/rust/library";
            };
            packages = [
              rustToolchain
              pkgs.sqlx-cli
              pkgs.sqldef
              pkgs.just
              pkgs.cargo-watch
              pkgs.nixpkgs-fmt
              pkgs.skopeo
            ];
          };
        };
    };
}
