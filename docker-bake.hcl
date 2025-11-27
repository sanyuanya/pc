group "default" {
  targets = ["pc"]
}

target "pc" {
  context    = "."
  dockerfile = "Dockerfile"
  platforms  = ["linux/amd64", "linux/arm64"]
  tags       = ["${IMAGE_REPO}:latest"]
}

