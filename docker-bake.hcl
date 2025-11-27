group "default" {
  targets = ["pc"]
}

variable "IMAGE_REPO" {
  default = "sanyuanya/pc"
}

target "pc" {
  context    = "."
  dockerfile = "Dockerfile"
  platforms  = ["linux/amd64", "linux/arm64"]
  tags       = ["${IMAGE_REPO}:latest", "${IMAGE_REPO}:{{ .git.shortSha }}"]
}
