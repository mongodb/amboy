# project configuration
name := grip
buildDir := build
packages := logging $(name)
projectPath := github.com/tychoish/$(name)


# declaration of dependencies
lintDeps := github.com/alecthomas/gometalinter
lintDeps += github.com/alecthomas/gocyclo
lintDeps += github.com/golang/lint/golint
lintDeps += github.com/gordonklaus/ineffassign
lintDeps += github.com/jgautheron/goconst/cmd/goconst
lintDeps += github.com/kisielk/errcheck
lintDeps += github.com/mdempsky/unconvert
lintDeps += github.com/mibk/dupl
lintDeps += github.com/mvdan/interfacer
lintDeps += github.com/opennota/check
lintDeps += github.com/tsenart/deadcode
lintDeps += github.com/client9/misspell
lintDeps += github.com/walle/lll
lintDeps += honnef.co/go/simple
lintDeps += honnef.co/go/staticcheck
testDeps := github.com/stretchr/testify
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
lint:$(gopath)/src/$(projectPath) $(lintDeps)
	$(gopath)/bin/gometalinter $(lintArgs) ./... | sed 's%$</%%'
lint-deps:$(lintDeps)
test-deps:$(testDeps)
build:$(deps) $(srcFiles) $(gopath)/src/$(projectPath)
	$(vendorGopath) go build $(subst $(name),,$(subst -,/,$(foreach pkg,$(packages),./$(pkg))))
build-race:$(deps) $(srcFiles) $(gopath)/src/$(projectPath)
	$(vendorGopath) go build -race $(subst -,/,$(foreach pkg,$(packages),./$(pkg)))
test:$(testOutput)
race:$(raceOutput)
coverage:$(coverageOutput)
coverage-html:$(coverageHtmlOutput)
phony := lint build build-race race test coverage coverage-html
phony += test-deps lint-deps test-deps
.PRECIOUS: $(testOutput) $(raceOutput) $(coverageOutput) $(coverageHtmlOutput)
# end front-ends


# implementation details for building the binary and creating a
# convienent link in the working directory
$(name):$(buildDir)/$(name)
	@[ -e $@ ] || ln -s $<
$(buildDir)/$(name):$(srcFiles)
	$(vendorGopath) go build -o $@ main/$(name).go
$(buildDir)/$(name).race:$(srcFiles)
	$(vendorGopath) go build -race -o $@ main/$(name).go
phony += $(buildDir)/$(name)
# end main build


# convenience targets for runing tests and coverage tasks on a
# specific package.
makeArgs := --no-print-directory
race-%:
	@$(MAKE) $(makeArgs) $(buildDir)/race.$(subst -,/,$*).out
	@grep -s -q -e "^PASS" $(buildDir)/race.$(subst -,/,$*).out
test-%:
	@$(MAKE) $(makeArgs) $(buildDir)/test.$(subst -,/,$*).out
	grep -e "^PASS" $(buildDir)/test.$(subst /,-,$*).out
coverage-%:
	@$(MAKE) $(makeArgs) $(buildDir)/coverage.$(subst -,/,$*).out
html-coverage-%:
	@$(MAKE) $(makeArgs) $(buildDir)/coverage.$(subst -,/,$*).html
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
#    tests have compile and runtime deps. This varable has everything
#    that the tests actually need to run. (The "build" target is
#    intentional and makes these targets rerun as expected.)
testRunDeps := $(testSrcFiles) build
testArgs := -v --timeout=20m
#    implementation for package coverage and test running, to produce
#    and save test output.
$(buildDir)/coverage.%.html:$(buildDir)/coverage.%.out
	go tool cover -html=$< -o $(subst -,/,$@)
$(buildDir)/coverage.%.out:$(testRunDeps)
	$(vendorGopath) go test -covermode=count -coverprofile=$@ $(projectPath)/$(subst -,/,$*)
	@-[ -f $@ ] && go tool cover -func=$@ | sed 's%$(projectPath)/%%' | column -t
$(buildDir)/coverage.$(name).out:$(testRunDeps)
	$(vendorGopath) go test -covermode=count -coverprofile=$@ $(projectPath)
	@-[ -f $@ ] && go tool cover -func=$@ | sed 's%$(projectPath)/%%' | column -t
$(buildDir)/test.%.out:$(testRunDeps)
	$(vendorGopath) go test $(testArgs) ./$(subst -,/,$*) | tee $(buildDir)/test.$(subst /,-,$*).out
$(buildDir)/race.%.out:$(testRunDeps)
	$(vendorGopath) go test $(testArgs) -race ./$(subst -,/,$*) | tee $(buildDir)/race.$(subst /,-,$*).out
$(buildDir)/test.$(name).out:$(testRunDeps)
	$(vendorGopath) go test $(testArgs) ./ | tee $@
$(buildDir)/race.$(name).out:$(testRunDeps)
	$(vendorGopath) go test $(testArgs) -race ./ | tee $@
# end test and coverage artifacts


# clean and other utility targets
clean:
	rm -rf $(name) $(deps) $(lintDeps) $(testDeps) $(buildDir)/test.* $(buildDir)/coverage.* $(buildDir)/race.*
phony += clean
# end dependency targets

# configure phony targets
.PHONY:$(phony)
