# start project configuration
name := amboy
buildDir := build
packages := dependency job registry pool queue
orgPath := github.com/mongodb
projectPath := $(orgPath)/$(name)
# end project configuration

# start dependency declarations
#   package, testing, and linter dependencies specified
#   separately. This is a temporary solution: eventually we should
#   vendorize all of these dependencies.
lintDeps := github.com/alecthomas/gometalinter
lintDeps += github.com/alecthomas/gocyclo
lintDeps += github.com/golang/lint/golint
lintDeps += github.com/gordonklaus/ineffassign
lintDeps += github.com/jgautheron/goconst/cmd/goconst
lintDeps += github.com/kisielk/errcheck
lintDeps += github.com/mdempsky/unconvert
lintDeps += github.com/mibk/dupl
lintDeps += github.com/mvdan/interfacer/cmd/interfacer
lintDeps += github.com/opennota/check/cmd/aligncheck
lintDeps += github.com/opennota/check/cmd/structcheck
lintDeps += github.com/opennota/check/cmd/varcheck
lintDeps += github.com/tsenart/deadcode
lintDeps += github.com/client9/misspell/cmd/misspell
lintDeps += github.com/walle/lll/cmd/lll
lintDeps += honnef.co/go/simple/cmd/gosimple
lintDeps += honnef.co/go/staticcheck/cmd/staticcheck
#   test dependencies.
testDeps := github.com/stretchr/testify
testDeps += github.com/satori/go.uuid
#   package dependencies.
deps := github.com/tychoish/grip
deps += github.com/gonum/graph
deps += github.com/gonum/matrix
deps += github.com/gonum/floats
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
lintArgs += --exclude="don.t use underscores.*_DependencyState.*"
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
build:$(deps) $(srcFiles) $(gopath)/src/$(projectPath)
	go build $(foreach pkg,$(packages),./$(pkg))
build-race:$(deps) $(srcFiles) $(gopath)/src/$(projectPath)
	go build -race $(foreach pkg,$(packages),./$(pkg))
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
	go build -o $@ main/$(name).go
$(buildDir)/$(name).race:$(gopath)/src/$(projectPath) $(srcFiles) $(deps)
	go build -race -o $@ main/$(name).go
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

# start test and coverage artifacts
#    tests have compile and runtime deps. This varable has everything
#    that the tests actually need to run. (The "build" target is
#    intentional and makes these targets rerun as expected.)
testRunDeps := $(testDeps) $(testSrcFiles) $(deps) build
#    implementation for package coverage and test running,mongodb to produce
#    and save test output.
$(buildDir)/coverage.%.html:$(buildDir)/coverage.%.out
	go tool cover -html=$< -o $@
$(buildDir)/coverage.%.out:$(testRunDeps)
	go test -covermode=count -coverprofile=$@ $(projectPath)/$*
	@-[ -f $@ ] && go tool cover -func=$@ | sed 's%$(projectPath)/%%' | column -t
$(buildDir)/coverage.$(name).out:$(testRunDeps)
	go test -covermode=count -coverprofile=$@ $(projectPath)
	@-[ -f $@ ] && go tool cover -func=$@ | sed 's%$(projectPath)/%%' | column -t
$(buildDir)/test.%.out:$(testRunDeps)
	go test -v ./$* >| $@; exitCode=$$?; cat $@; [ $$exitCode -eq 0 ]
$(buildDir)/test.$(name).out:$(testRunDeps)
	go test -v ./ >| $@; exitCode=$$?; cat $@; [ $$exitCode -eq 0 ]
$(buildDir)/race.%.out:$(testRunDeps)
	go test -race -v ./$* >| $@; exitCode=$$?; cat $@; [ $$exitCode -eq 0 ]
$(buildDir)/race.$(name).out:$(testRunDeps)
	go test -race -v ./ >| $@; exitCode=$$?; cat $@; [ $$exitCode -eq 0 ]
# end test and coverage artifacts


# clean and other utility targets
clean:
	rm -rf $(name) $(deps) $(lintDeps) $(testDeps) $(buildDir)/test.* $(buildDir)/coverage.* $(buildDir)/race.*
phony += clean
# end dependency targets

# configure phony targets
.PHONY:$(phony)
