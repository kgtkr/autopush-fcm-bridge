{ lib, ... }: {
  perSystem = { self', pkgs, ... }:
    let
      args = {
        name = "autopush-fcm-bridge";
        tag = "latest";
        contents = [
          pkgs.cacert
        ];
        fakeRootCommands = ''
          mkdir -p tmp
          chmod 1777 tmp
        '';
        config = {
          Entrypoint = [ (lib.getExe self'.packages.autopush-fcm-bridge) ];
          Env = [
            "HOST=0.0.0.0"
            "PORT=8080"
            "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
          ];
          ExposedPorts = {
            "8080/tcp" = { };
          };
          User = "65534:65534";
          WorkingDir = "/";
        };
      };
    in
    {
      packages = {
        image = pkgs.dockerTools.buildLayeredImage args;
      };
      apps = {
        streamImage = {
          type = "app";
          program = "${pkgs.dockerTools.streamLayeredImage args}";
        };
      };
    };
}
