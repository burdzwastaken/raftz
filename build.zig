const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const raft_module = b.addModule("raftz", .{
        .root_source_file = b.path("src/raftz.zig"),
        .target = target,
        .optimize = optimize,
    });

    addExample(b, target, optimize, raft_module, "examples/client.zig", "raft-client", "run-client", "Run the client example");
    addExample(b, target, optimize, raft_module, "examples/cluster.zig", "raft-cluster", "run-cluster", "Run the cluster example");
    addExample(b, target, optimize, raft_module, "examples/simple.zig", "raft-simple", "run-simple", "Run the simple example");

    const test_module = b.createModule(.{
        .root_source_file = b.path("src/raftz.zig"),
        .target = target,
        .optimize = optimize,
    });
    const lib_tests = b.addTest(.{
        .root_module = test_module,
    });
    const run_lib_tests = b.addRunArtifact(lib_tests);

    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&run_lib_tests.step);

    addIntegrationTest(b, target, optimize, raft_module, "tests/test_utils.zig", "test-utils", test_step, "Test utilities");
    addIntegrationTest(b, target, optimize, raft_module, "tests/leader_election_test.zig", "test-election", test_step, "Test leader elections");
    addIntegrationTest(b, target, optimize, raft_module, "tests/log_replication_test.zig", "test-replication", test_step, "Test log replication");
    addIntegrationTest(b, target, optimize, raft_module, "tests/safety_test.zig", "test-safety", test_step, "Test safety properties");

    const docs_module = b.createModule(.{
        .root_source_file = b.path("src/raftz.zig"),
        .target = target,
        .optimize = optimize,
    });
    const docs_obj = b.addObject(.{
        .name = "raftz",
        .root_module = docs_module,
    });
    const install_docs = b.addInstallDirectory(.{
        .source_dir = docs_obj.getEmittedDocs(),
        .install_dir = .prefix,
        .install_subdir = "docs",
    });

    const docs_step = b.step("docs", "Generate documentation");
    docs_step.dependOn(&install_docs.step);

    const serve_docs_module = b.createModule(.{
        .root_source_file = b.path("tools/serve_docs.zig"),
        .target = target,
        .optimize = optimize,
    });
    const serve_docs_exe = b.addExecutable(.{
        .name = "serve",
        .root_module = serve_docs_module,
    });
    b.installArtifact(serve_docs_exe);

    const serve_docs_cmd = b.addRunArtifact(serve_docs_exe);
    serve_docs_cmd.step.dependOn(&install_docs.step);
    serve_docs_cmd.step.dependOn(b.getInstallStep());

    const serve_docs_step = b.step("serve", "Serve documentation on http://127.0.0.1:8080");
    serve_docs_step.dependOn(&serve_docs_cmd.step);
}

fn addExample(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    raft_module: *std.Build.Module,
    source_path: []const u8,
    exe_name: []const u8,
    step_name: []const u8,
    step_description: []const u8,
) void {
    const example_module = b.createModule(.{
        .root_source_file = b.path(source_path),
        .target = target,
        .optimize = optimize,
    });
    example_module.addImport("raftz", raft_module);

    const exe = b.addExecutable(.{
        .name = exe_name,
        .root_module = example_module,
    });
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const run_step = b.step(step_name, step_description);
    run_step.dependOn(&run_cmd.step);
}

fn addIntegrationTest(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    raft_module: *std.Build.Module,
    source_path: []const u8,
    name: []const u8,
    test_step: *std.Build.Step,
    description: []const u8,
) void {
    _ = description;
    const test_exe_module = b.createModule(.{
        .root_source_file = b.path(source_path),
        .target = target,
        .optimize = optimize,
    });
    test_exe_module.addImport("raftz", raft_module);

    const test_exe = b.addTest(.{
        .name = name,
        .root_module = test_exe_module,
    });

    const run_test = b.addRunArtifact(test_exe);
    test_step.dependOn(&run_test.step);
}
