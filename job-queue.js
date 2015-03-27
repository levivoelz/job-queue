var kue = require('kue'),
  rest = require('restler'),
  jobs = kue.createQueue() ,
  mongoose = require('mongoose');

// Setup DB and make the connection
var PostSchema = new mongoose.Schema({
  title: String,
  updated_at: { type: Date, default: Date.now },
});

mongoose.connect('mongodb://localhost/fetch', function(err) {
  if (err) throw "Error connecting to database";
});

// Show command the user should run
var curl = "curl -H 'Content-Type: application/json' -d "
           + "'{\"type\": \"fetch\", \"data\": {\"url\":"
           + "\"http://jsonplaceholder.typicode.com/posts/\","
           + "\"post_id\": \"3\"}}' "
           + "http://localhost:3000/job";

           console.log("Run a curl command to create a job: ~$", curl);

// Handle Jobs
jobs.process('fetch', function (job, done){
  rest.get(job.data.url + job.data.post_id)
    .on('complete', function(result) {
    
      if (result instanceof Error) {
        handleError(result.message);
        this.retry(5000);
      } else {
      
        if (result.length != 0) {
          createPost(result, done);
          console.log("Job:", job.id, "was created. "
                      + "Go to http://localhost:3000/job/" 
                      + job.id, "for job status.");
        } else {
          console.log("ERROR: No results returned.");
        }
      }
    });
});

// Save post to DB
function createPost(result, done) {
  var Post = mongoose.model('Post', PostSchema);
  var post = new Post({title: result.title});
  
  post.save(function(err){
    if(err) {
      console.log(err);
    }
    else {
      console.log(result.title, "was saved.")
    }
  });

  // Optionally show all posts to confirm on save.
  showAllPosts(Post);
  done && done();
}

function showAllPosts(post) {
  post.find(function (err, posts) {
    console.log("Save successful. Here are all posts: ");
    console.log(posts);
  });
}

function handleError(message) {
  console.log('Error:', message);
}

kue.app.listen(3000);