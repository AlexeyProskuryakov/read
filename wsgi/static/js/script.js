function start_find_comments(sub){
    console.log(sub);
    $.ajax({
        type:"POST",
        url:"/comment_search/start/"+sub,
        success:function(x){
             console.log(x);
             $("#"+sub+"-st").text(x.state);
        }
    })
}

function refresh_searcher_state(sub){
        console.log(sub);
    $.ajax({
        type:"POST",
        url:"/comment_search/reset_state/"+sub,
        success:function(x){
             console.log(x);
             $("#"+sub+"-st").text("Состояние сброшено");
        }
    })
}

