/* Hello OpenWhisk in JavaScript. */
function main(args) {
    var name = args.name || "stranger";

    return { "greeting" : "Hello " + name + " !" };
}
