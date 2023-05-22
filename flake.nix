{
  description = "Yo";

  inputs.nixpkgs.url      = "github:nixos/nixpkgs/nixos-unstable";
  inputs.flake-utils.url  = "github:numtide/flake-utils";
  inputs.rust-overlay.url = "github:oxalica/rust-overlay";

  outputs = { self, nixpkgs, flake-utils, rust-overlay, ... }:
    flake-utils.lib.eachDefaultSystem (system:
    let
      overlays = [ rust-overlay.overlays.default
        (final: prev: {
          cargo = final.rust-bin.nightly.latest.default;
          rustc = final.rust-bin.nightly.latest.default;
        })
      ];
      pkgs = import nixpkgs { inherit system overlays; };
    in {
      devShell = pkgs.mkShell {
        nativeBuildInputs = with pkgs; [
          rustc cargo pkgconfig clippy rust-analyzer
          mongodb-tools  # bsondump
        ];
        buildInputs = with pkgs; [
          openssl.dev
        ]
        ++ lib.optional stdenv.isDarwin darwin.apple_sdk.frameworks.Security
        ++ lib.optional stdenv.isDarwin darwin.apple_sdk.frameworks.AppKit
        ;
        RUST_SRC_PATH = "${pkgs.rust-bin.nightly.latest.rust-src}/lib/rustlib/src/rust/library";
        # mold does not seem to use pkg-config with openssl.dev
        # LD_LIBRARY_PATH = "${pkgs.openssl.out}/lib";  XXX: causes glibc version mismatch
        # RUST_SRC_PATH = "${pkgs.rust.packages.nightly.rustPlatform.rustLibSrc}";
      };
    });
}
