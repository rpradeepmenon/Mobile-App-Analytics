#### Function LoadPackage
loadPackage <- function(packages) {
        # Installs the required packages
        # 
        # Args:
        #       packages: The list of packages to be installed
        #       
        # Returns:
        #       Message that the packages are installed
        #       
        # Error Handling
        n <- length(packages)
        if (n < 1) {
                stop("The package list is empty")
        }
        
        if (length(setdiff(packages, rownames(installed.packages()))) > 0) {
                install.packages(setdiff(packages, rownames(installed.packages())))  
        }
        for (package_name in packages) {
                library(package_name, character.only = TRUE, quietly = TRUE, verbose = FALSE)  
        }        
        return("Packages installed and loaded successfully")
}

#### Function Plot Count
plotBar <- function(data, x, y) {
        # Creates Bar Plots
        # 
        # Args:
        #       data: The dataset based on which bar plot needs to be created
        #       x: X-axis data for the bar plot
        #       y: Y-axis data for the bar plot
        #       
        # Returns:
        #       A bar plot
        require(ggplot2)
        
        xcol <- deparse(substitute(x))
        ycol <- deparse(substitute(y))
        title <- paste("Analysis of ", xcol, "vs", ycol)
        ggplot(data, aes_string(x = sprintf("factor(%s)", xcol), y = ycol)) + geom_bar(stat = "identity", fill = "black", 
                                                                                       width = 0.5) +
                ylab(ycol) + xlab(xcol) + theme_tufte() + ggtitle(title)
}

#### Function to Plot Histogram
plotHist <- function(data, x, bin_width = 30){
        # Creates a Histogram
        # 
        # Args:
        #       data: The dataset based on which histogram needs to be created
        #       x: X-axis data for the histogram
        #       
        # Returns:
        #       A Histogram
        require(ggplot2)
        xcol <- deparse(substitute(x))
        h <- ggplot(data, aes_string(x = xcol)) + 
                geom_histogram(binwidth = bin_width, col = "red", fill = "green") + 
                theme_tufte() + ggtitle(paste0("Histogram of ", xcol))
        return(h)
}


#### Function to Plot Facetted Histogram
plotHistFacet <- function(data, x, bin_width = 30, facet_var){
        # Creates a Histogram
        # 
        # Args:
        #       data: The dataset based on which histogram needs to be created
        #       x: X-axis data for the histogram
        #       facet_var: variable for faceting
        #       
        # Returns:
        #       A Histogram with facets
        require(ggplot2)
        xcol <- deparse(substitute(x))
        facet_col <- deparse(substitute(facet_var))
        h <- ggplot(data, aes_string(x = xcol)) + 
                geom_histogram(binwidth = bin_width, col = "blue", fill = "black") + 
                theme_tufte() + facet_wrap(as.formula(paste("~", facet_col))) +
                ggtitle(paste0("Histogram of ", xcol, " by ", facet_col))
        return(h)
}


#### Function to Plot Boxplot
plotBox <- function(data, x, y) {
        # Creates a Boxplot
        # 
        # Args:
        #       data: The dataset based on which histogram needs to be created
        #       x: X-axis data for the boxplot
        #       y: Y-axis data for the boxplot
        #       
        # Returns:
        #       A box plot
        require(ggplot2)
        xcol <- deparse(substitute(x))
        ycol <- deparse(substitute(y))
        h <- ggplot(data, aes_string(x = sprintf("factor(%s)", xcol), y = ycol)) + 
                geom_boxplot(outlier.color = "red", col = "green", fill = "black") + 
                theme_tufte() + ggtitle(paste0("Boxplot of ", ycol, " vs ", xcol)) + xlab(xcol)
        return(h)
}



#### Function to Scatter Plot
scatterPlot <- function(data, xvar, yvar) {
        # Creates Scatter Plots
        # 
        # Args:
        #       data: The dataset based on which scatter plot needs to be created
        #       x: X-axis data for the scatter plot
        #       y: Y-axis data for the scatter plot
        #       
        # Returns:
        #       A scatter plot
        df <- data
        xval <- deparse(substitute(xvar))
        yval <- deparse(substitute(yvar))
        ftx <- df[[xval]]
        fty <- df[[yval]]
        corr <- cor(ftx, fty)
        scatterplot <- ggplot(data, aes_string(x = sprintf("as.numeric(%s)", xval), y = sprintf("as.numeric(%s)", yval))) +
                geom_point(shape = 1) +    # Use hollow circles
                geom_smooth()         +    # Add a loess smoothed fit curve with confidence region
                xlab(xval) + ylab(yval) + theme_tufte() + 
                 annotate("text", 
                           label = paste0("corr = ",corr),
                           x = max(ftx) - sd(ftx),
                           y = 30)
        
        
        return(scatterplot)
        
}

#### Tabulate NA Values
tableNA <- function(data) {
        # Tabluates NA values in a data set
        # 
        # Args:
        #       data: The data frame on which NAs needs to be tabulated
        #       
        # Returns:
        #       A data frame with the count analysis
        NADF <- data.frame(col.name = factor(), no.of.na = integer(), percent.of.na = integer())
        nrows <- nrow(data)
        cols <- colnames(data)
        ncols <- length(cols)
        for (i in 1:ncols) {
                ColName <- cols[i]
                NoOfNA <- sum(is.na(data[,cols[i]]))
                PctOfNA <- NoOfNA/nrows * 100
                TempDf <- data.frame(col.name = ColName, no.of.na = NoOfNA, percent.of.na = PctOfNA)
                NADF <- rbind(NADF, TempDf)
        }
        NADF <- subset(NADF, no.of.na > 0)
        return(NADF)
}
#### Get Stats
getStats <- function(data, var) {
        # Computes Statistics for the variable
        # 
        # Args:
        #       data: The dataset based on which correlation matrix needs to be created
        #       var: Variable for which statistics needs to be computed
        #       
        # Returns:
        #       A data frame with statistics for the variable
        require(psych)
        df <- data
        var <- deparse(substitute(var))
        ft <- df[[var]]
        #sum_stats <- as.data.frame(matrix(summary(ft)))
        count <- length(ft)
        na <- sum(is.na(ft))
        min <- min(ft)
        max <- max(ft)
        mean <- mean(ft)
        median <- median(ft)
        range <- max(ft) - min(ft)
        sd <- sd(ft)
        variance <- var(ft)
        skewness <- skew(ft)
        kurtosis <- kurtosi(ft)
        sum_stats <- as.data.frame(t(data.frame(count, na, min, max, mean, median, range, sd, variance, skewness, 
                                                kurtosis)))
        names(sum_stats) <- "Value" 
        return(sum_stats)
        
}

# Get Correlation
getCorr <- function(data, varlist){
        # Creates a correlation matrix
        # 
        # Args:
        #       data: The dataset based on which correlation matrix needs to be created
        #       varlist: A list of numeric variable column names
        #       
        # Returns:
        #       A correlation matrix for the variables
        require(corrplot)
        require(caret)
        data_cor <- data[, varlist]
        corr_var <- cor(data_cor)
        cor_plot <- corrplot(corr_var, order = "hclust", tl.cex = 0.4)
        cor_plot_mix <- corrplot.mixed(corr_var, order = "hclust", tl.cex = 0.4)
        cor_out  <- list(cor_plot, cor_plot_mix)
        return(cor_out)
        
        
}

# Evaluate Liner Regression
evalLM <- function(actualvar, predictvar){
        # Evaluates a Linear Regression Model
        # 
        # Args:
        #       actualvar: A vecotr of variables with actual values
        #       predictvar: A vecotr of variables with predicted values
        #       
        # Returns:
        #       A dataframe with the following evaluation metrics: 
        #               Root Mean Squared Error
        #               Mean Squared Error
        #               R-Squared Value
        error <- predictvar - actualvar
        rmse <- sqrt(mean(error ^ 2))
        mse <- mean(abs(error))
        rsq <- 1 - sum((actualvar - predictvar) ^ 2)/sum((actualvar - mean(actualvar)) ^ 2)
        Eval_Tab <- as.data.frame(t(data.frame(rmse, mse, rsq)))
        colnames(Eval_Tab) <- "values"
        return(Eval_Tab)
        
}

# Function that replaces all NA values with last non-NA values
replaceNA <- function(x) {
    na.lomf.0 <- function(x) {
        non.na.idx <- which(!is.na(x))
        if (is.na(x[1L])) {
            non.na.idx <- c(1L, non.na.idx)
        }
        rep.int(x[non.na.idx], diff(c(non.na.idx, length(x) + 1L)))
    }
    
    dim.len <- length(dim(x))
    
    if (dim.len == 0L) {
        na.lomf.0(x)
    } else {
        apply(x, dim.len, na.lomf.0)
    }
}
