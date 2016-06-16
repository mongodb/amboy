# project configuration
name := grip
buildDir := build
packages := logging grip # TODO: add other packages when there are tests
projectPath := github.com/tychoish/$(name)


# declaration of dependencies
lintDeps := github.com/alecthomas/gometalinter
testDeps := github.com/stretchr/testify
deps := github.com/coreos/go-systemd/journal
# end dependency declarations


# start linting configuration
#   include test files and give linters 40s to run to avoid timeouts
lintArgs := --deadline=40s --tests
#   skip the build directory and the gopath,
lintArgs += --skip="$(gopath)" --skip="$(buildDir)"
#  the go type package produces false positives for (sub)packages,
#  because it doesn't parse dependency information from compiled go
#  resources correctly.
lintArgs += --disable="gotype"
#  add and configure additional linters
lintArgs += --enable="go fmt -s" --enable="goimports"
lintArgs += --linter='misspell:misspell ./*.go:PATH:LINE:COL:MESSAGE' --enable=misspell
lintArgs += --line-length=100 --dupl-threshold=100 --cyclo-over=15
#  two similar functions triggered the duplicate warning, but they're not.
lintArgs += --exclude="duplicate of registry.go"
#  golint doesn't handle splitting package comments between multiple files.
lintArgs += --exclude="package comment should be of the form \"Package .* \(golint\)"
# end lint suppressions


######################################################################
##
## Everything below this point is generic, and does not contain
## project specific configuration. (with one noted case in the "build"
## target for library-only projects)
##
######################################################################


# start dependency installation tools
#   implementation details for being able to lazily install dependencies
gopath := $(shell go env GOPATH)
deps := $(addprefix $(gopath)/src/,$(deps))
lintDeps := $(addprefix $(gopath)/src/,$(lintDeps))
testDeps := $(addprefix $(gopath)/src/,$(testDeps))
srcFiles := makefile $(shell find . -name "*.go" -not -path "./$(buildDir)/*" -not -name "*_test.go")
testSrcFiles := makefile $(shell find . -name "*.go" -not -path "./$(buildDir)/*")
testOutput := $(foreach target,$(packages),$(buildDir)/test.$(target).out)
raceOutput := $(foreach target,$(packages),$(buildDir)/race.$(target).out)
coverageOutput := $(foreach target,$(packages),$(buildDir)/coverage.$(target).out)
coverageHtmlOutput := $(foreach target,$(packages),$(buildDir)/coverage.$(target).html)
$(gopath)/src/%:
	@-[ ! -d $(gopath) ] && mkdir -p $(gopath) || true
	go get $(subst $(gopath)/src/,,$@)
# end dependency installation tools


# userfacing targets for basic build and development operations
lint:$(gopath)/src/$(projectPath) $(lintDeps) $(deps)
	$(gopath)/bin/gometalinter $(lintArgs) ./... | sed 's%$</%%'
deps:$(deps)
test-deps:$(testDeps)
lint-deps:$(lintDeps)
build:$(deps) $(gopath)/src/$(projectPath)
	$(vendorGopath) go build ./ $(foreach pkg,$(shell find . -type d -not -iwholename '*.git*' -not -iwholename "*vendor*" -not -iwholename "*buildscripts*" -not -name "." -not -name "build"),$(pkg))
test:$(testOutput)
race:$(raceOutput)
coverage:$(coverageOutput)
coverage-html:$(coverageHtmlOutput)
phony := lint build build-race race test coverage coverage-html
phony += deps test-deps lint-deps
.PRECIOUS: $(testOutput) $(raceOutput) $(coverageOutput) $(coverageHtmlOutput)
# end front-ends


# implementation details for building the binary and creating a
# convienent link in the working directory
$(gopath)/src/$(orgPath):
	@mkdir -p $@
$(gopath)/src/$(projectPath):$(gopath)/src/$(orgPath)
	@[ -L $@ ] || ln -s $(shell pwd) $@
$(name):$(buildDir)/$(name)
	@[ -L $@ ] || ln -s $< $@
$(buildDir)/$(name):$(gopath)/src/$(projectPath) $(srcFiles) $(deps)
	$(vendorGopath) go build -o $@ main/$(name).go
$(buildDir)/$(name).race:$(gopath)/src/$(projectPath) $(srcFiles) $(deps)
	$(vendorGopath) go build -race -o $@ main/$(name).go
# end main build


# convenience targets for runing tests and coverage tasks on a
# specific package.
makeArgs := --no-print-directory
race-%:
	@$(MAKE) $(makeArgs) $(buildDir)/race.$*.out
test-%:
	@$(MAKE) $(makeArgs) $(buildDir)/test.$*.out
coverage-%:
	@$(MAKE) $(makeArgs) $(buildDir)/coverage.$*.out
html-coverage-%:
	@$(MAKE) $(makeArgs) $(buildDir)/coverage.$*.html
# end convienence targets

# start vendoring configuration
#    begin with configuration of dependencies
vendorDeps := github.com/Masterminds/glide
vendorDeps := $(addprefix $(gopath)/src/,$(vendorDeps))
vendor-deps:$(vendorDeps)
#   this allows us to store our vendored code in vendor and use
#   symlinks to support vendored code both in the legacy style and with
#   new-style vendor directories. When this codebase can drop support
#   for go1.4, we can delete most of this.
-include $(buildDir)/makefile.vendor
$(buildDir)/makefile.vendor:$(buildDir)/render-gopath
	@mkdir -p $(buildDir)
	@echo "vendorGopath := \$$(shell \$$(buildDir)/render-gopath)" >| $@
#   targets for the directory components and manipulating vendored files.
vendor-sync:$(vendorDeps)
	glide install -s
vendor-clean:
#   clean binary files (makes patch builds possible and avoids commiting some things.)
	find vendor/ -name "*.gif" -o -name "*.gz" -o -name "*.png" -o -name "*.ico" | xargs rm -f
#   retain a minor edit to vendored code to add compatibility for gccgo.
	rm -f vendor/golang.org/x/tools/container/intsets/popcnt_gccgo*
	git checkout vendor/golang.org/x/tools/container/intsets/popcnt_generic.go
change-go-version:
	rm -rf $(buildDir)/make-vendor $(buildDir)/render-gopath
	@$(MAKE) $(makeArgs) vendor > /dev/null 2>&1
vendor:$(buildDir)/vendor/src
$(buildDir)/vendor/src:$(buildDir)/make-vendor $(buildDir)/render-gopath
	@./$(buildDir)/make-vendor
#   targets to build the small programs used to support vendoring.
$(buildDir)/make-vendor:buildscripts/make-vendor.go
	@mkdir -p $(buildDir)
	go build -o $@ $<
$(buildDir)/render-gopath:buildscripts/render-gopath.go
	@mkdir -p $(buildDir)
	go build -o $@ $<
#   define dependencies for buildscripts
buildscripts/make-vendor.go:buildscripts/vendoring/vendoring.go
buildscripts/render-gopath.go:buildscripts/vendoring/vendoring.go
#   add phony targets
phony += vendor vendor-deps vendor-clean vendor-sync change-go-version
# end vendoring tooling configuration


# start test and coverage artifacts
#    This varable includes everything that the tests actually need to
#    run. (The "build" target is intentional and makes these targetsb
#    rerun as expected.)
testRunDeps := $(testSrcFiles) build
#    implementation for package coverage and test running, to produce
#    and save test output.
$(buildDir)/coverage.%.html:$(buildDir)/coverage.%.out
	$(vendorGopath) go tool cover -html=$< -o $@
$(buildDir)/coverage.%.out:$(testRunDeps)
	$(vendorGopath) go test -covermode=count -coverprofile=$@ $(projectPath)/$*
	@-[ -f $@ ] && go tool cover -func=$@ | sed 's%$(projectPath)/%%' | column -t
$(buildDir)/coverage.$(name).out:$(testRunDeps)
	$(vendorGopath) go test -covermode=count -coverprofile=$@ $(projectPath)
	@-[ -f $@ ] && go tool cover -func=$@ | sed 's%$(projectPath)/%%' | column -t
$(buildDir)/test.%.out:$(testRunDeps)
	$(vendorGopath) go test -v ./$* >| $@; exitCode=$$?; cat $@; [ $$exitCode -eq 0 ]
$(buildDir)/test.$(name).out:$(testRunDeps)
	$(vendorGopath) go test -v ./ >| $@; exitCode=$$?; cat $@; [ $$exitCode -eq 0 ]
$(buildDir)/race.%.out:$(testRunDeps)
	$(vendorGopath) go test -race -v ./$* >| $@; exitCode=$$?; cat $@; [ $$exitCode -eq 0 ]
$(buildDir)/race.$(name).out:$(testRunDeps)
	$(vendorGopath) go test -race -v ./ >| $@; exitCode=$$?; cat $@; [ $$exitCode -eq 0 ]
# end test and coverage artifacts


# clean and other utility targets
clean:
	rm -rf $(name) $(deps) $(lintDeps) $(testDeps) $(buildDir)/test.* $(buildDir)/coverage.* $(buildDir)/race.*
phony += clean
# end dependency targets

# configure phony targets
.PHONY:$(phony)
