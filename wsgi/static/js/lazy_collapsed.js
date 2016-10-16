$(".collapsed-lazy").on("show.bs.collapse", function(e, p){
    console.log("lazy collapse");
    console.log(this);
    var sub = this.getAttribute("sub");
    $.ajax({
        type:"GET",
        url:"/comments/queue/"+sub,
        success:function(x){
            console.log(x);
            if (x.posts != undefined){
                x.posts.forEach(function(post){
                    var post_info = `<tr id=comment-{{id}}>
                        <td><a href={{url}}>{{fullname}}</a></td>
                        <td>{{text}}</td>
                        <td>{{supplier}}</td>
                        <td><button class='btn btn-danger btn-sm' onclick="set_comment_bad('{{id}}')">Нахуй</button>
                        </tr>`;
                    var result = Mustache.render(post_info, post);
                    $("#"+sub+"-posts").append(result);
                });

            }
        }
    })
});